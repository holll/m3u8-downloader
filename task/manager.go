package task

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"m3u8-downloader/dl"
)

// ============================== Manager ==============================

// DefaultMaxWorkers 单任务默认下载线程数，与 dl 包的默认并发保持一致。
// 实测依据见 dl.DefaultMaxWorkers 的注释。
const DefaultMaxWorkers = dl.DefaultMaxWorkers

// ShutdownFunc 关闭回调函数类型（由 main 注入，RPC 触发时执行进程退出）。
type ShutdownFunc func()

// Manager 任务管理器
type Manager struct {
	tasks         map[string]*Task
	taskOrder     []string // GID 插入顺序，保证 FIFO 调度
	mu            sync.RWMutex
	activeLimit   int
	activeCount   int
	stoppedList   []string // GID 列表 (已完成/失败/已删除，FIFO)
	stoppedMax    int      // stoppedList 最大长度
	defaultWorker int      // 单任务默认下载线程数
	dir           string   // 全局默认下载目录（空=当前目录）

	onShutdown ShutdownFunc // aria2.shutdown 回调

	eventFn func(method, gid string) // 状态迁移事件回调（推送 aria2 通知）

	sessionFile string      // 会话文件路径（空=不保存）
	saveTimer   *time.Timer // 防抖定时器
	saveTimerMu sync.Mutex  // 保护 saveTimer
}

// NewManager 创建管理器
// dir 为全局默认下载目录（空字符串 → 自动取 "."）。
// sessionFile 为空时跳过会话持久化。
func NewManager(maxConcurrent, defaultWorkers int, dir, sessionFile string) *Manager {
	if maxConcurrent <= 0 {
		maxConcurrent = 5
	}
	if defaultWorkers <= 0 {
		defaultWorkers = DefaultMaxWorkers
	}
	if dir == "" {
		dir = "."
	}
	return &Manager{
		tasks:         make(map[string]*Task),
		taskOrder:     make([]string, 0),
		activeLimit:   maxConcurrent,
		stoppedMax:    1000,
		defaultWorker: defaultWorkers,
		dir:           dir,
		sessionFile:   sessionFile,
	}
}

// DefaultWorkers 返回单任务默认线程数
func (m *Manager) DefaultWorkers() int { return m.defaultWorker }

// ============================== 关闭回调 ==============================

// SetShutdownCallback 注入关闭回调（由 main.go 调用）。
func (m *Manager) SetShutdownCallback(fn ShutdownFunc) {
	m.onShutdown = fn
}

// RequestShutdown 触发优雅关闭（aria2.shutdown RPC 调用）。
func (m *Manager) RequestShutdown() {
	if m.onShutdown != nil {
		m.onShutdown()
	}
}

// ============================== 任务操作 ==============================

// AddURI 添加一个下载任务
func (m *Manager) AddURI(url string, opts Options) (string, error) {
	if url == "" {
		return "", fmt.Errorf("URL is required")
	}

	// 未指定目录时使用全局默认目录
	if opts.Dir == "" {
		opts.Dir = m.dir
	}

	t := NewTask(url, opts, m.onTaskUpdate)

	m.mu.Lock()
	m.tasks[t.GID] = t
	m.taskOrder = append(m.taskOrder, t.GID)
	m.mu.Unlock()

	// aria2 兼容：options.pause=true 时任务入队即暂停，等待 unpause 才启动
	if opts.Pause {
		_ = t.Pause()
		return t.GID, nil
	}

	// 尝试立即启动
	m.tryStartTask(t)
	return t.GID, nil
}

// Remove 移除任务
// 与 aria2 一致：仅 active/waiting/paused 任务可被移除；已停止（完成/失败/已移除）
// 的任务返回错误（对应 aria2 的 "Active Download not found for GID#..."），
// 避免重复写入 stoppedList 造成列表重复项。
func (m *Manager) Remove(gid string) error {
	t, err := m.getTask(gid)
	if err != nil {
		return err
	}

	s := t.Status()
	if s != StatusActive && s != StatusWaiting && s != StatusPaused {
		return fmt.Errorf("Active Download not found for GID#%s", gid)
	}

	wasActive := s == StatusActive
	t.Remove()

	if wasActive {
		m.mu.Lock()
		m.activeCount--
		m.mu.Unlock()
		m.tryStartNext()
	}

	// 移入 stopped 列表
	m.pushStopped(gid)
	return nil
}

// RemoveDownloadResult 清除已完成的记录
func (m *Manager) RemoveDownloadResult(gid string) error {
	t, err := m.getTask(gid)
	if err != nil {
		return err
	}
	s := t.Status()
	if s != StatusComplete && s != StatusError && s != StatusRemoved {
		return fmt.Errorf("task %s is not stopped/complete", gid)
	}
	m.mu.Lock()
	delete(m.tasks, gid)
	m.taskOrder = removeGID(m.taskOrder, gid)
	m.stoppedList = removeGID(m.stoppedList, gid)
	m.mu.Unlock()
	return nil
}

// removeGID 从 GID 列表中移除指定项（保持其余元素顺序）。
func removeGID(list []string, gid string) []string {
	for i, id := range list {
		if id == gid {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

// Pause 暂停任务
func (m *Manager) Pause(gid string) error {
	t, err := m.getTask(gid)
	if err != nil {
		return err
	}
	if t.Status() == StatusActive {
		m.mu.Lock()
		m.activeCount--
		m.mu.Unlock()
		m.tryStartNext()
	}
	return t.Pause()
}

// Unpause 恢复任务
func (m *Manager) Unpause(gid string) error {
	t, err := m.getTask(gid)
	if err != nil {
		return err
	}
	if err := t.Unpause(); err != nil {
		return err
	}
	m.tryStartTask(t)
	return nil
}

// PauseAll 暂停所有任务（aria2 的 pauseAll 同时暂停 active 与 waiting 队列）。
// 先暂停 waiting 队列，避免暂停 active 释放槽位后 tryStartNext 又启动新任务。
func (m *Manager) PauseAll() {
	m.mu.RLock()
	waiting := make([]*Task, 0)
	active := make([]*Task, 0)
	for _, gid := range m.taskOrder {
		t, ok := m.tasks[gid]
		if !ok {
			continue
		}
		switch t.Status() {
		case StatusWaiting:
			waiting = append(waiting, t)
		case StatusActive:
			active = append(active, t)
		}
	}
	m.mu.RUnlock()

	for _, t := range waiting {
		m.Pause(t.GID)
	}
	for _, t := range active {
		m.Pause(t.GID)
	}
}

// UnpauseAll 恢复所有暂停任务（受 activeLimit 限制）
func (m *Manager) UnpauseAll() {
	m.mu.RLock()
	paused := make([]*Task, 0)
	for _, gid := range m.taskOrder {
		if t, ok := m.tasks[gid]; ok && t.Status() == StatusPaused {
			paused = append(paused, t)
		}
	}
	m.mu.RUnlock()

	for _, t := range paused {
		m.Unpause(t.GID)
	}
}

// ============================== 查询 ==============================

// Status 返回任务快照
func (m *Manager) Status(gid string) (TaskStatus, error) {
	t, err := m.getTask(gid)
	if err != nil {
		return TaskStatus{}, err
	}
	return t.Snapshot(), nil
}

// TellActive 活跃任务列表
func (m *Manager) TellActive() []TaskStatus {
	return m.collect(StatusActive)
}

// paginate 复刻 aria2 AbstractPaginationRpcMethod::getPaginationRange 的分页语义。
// offset < 0 表示从队尾倒数；此时返回的切片在调用方需逆序（aria2 会 reverse）。
// num <= 0 返回空（与 aria2 IntegerGE(0) + num<=0 → empty 一致）。
func (m *Manager) paginate(total, offset, num int) (start, end int, reversed bool) {
	if num <= 0 {
		return 0, 0, false
	}
	if offset < 0 {
		tempoffset := offset + total
		if tempoffset < 0 {
			return 0, 0, false
		}
		start = tempoffset - (num - 1)
		if start < 0 {
			start = 0
			num = tempoffset + 1
		}
		end = start + num
		if end > total {
			end = total
		}
		return start, end, true
	}
	if total <= offset {
		return 0, 0, false
	}
	end = offset + num
	if end > total {
		end = total
	}
	return offset, end, false
}

// TellWaiting 等待队列（aria2 兼容：包含 waiting 和 paused 任务）
func (m *Manager) TellWaiting(offset, num int) []TaskStatus {
	// 先 waiting 后 paused，保持插入顺序
	all := m.collect(StatusWaiting)
	all = append(all, m.collect(StatusPaused)...)
	total := len(all)
	// aria2 对空结果返回 []（JSON 数组），而非 null
	if total == 0 {
		return []TaskStatus{}
	}
	start, end, reversed := m.paginate(total, offset, num)
	if start >= end {
		return []TaskStatus{}
	}
	result := make([]TaskStatus, 0, end-start)
	if reversed {
		for i := end - 1; i >= start; i-- {
			result = append(result, all[i])
		}
	} else {
		result = append(result, all[start:end]...)
	}
	return result
}

// TellStopped 已完成/失败/已删除列表。
// 顺序与 aria2 一致：offset<0 时从队尾倒数取 num 条并逆序返回（最新完成在前）。
func (m *Manager) TellStopped(offset, num int) []TaskStatus {
	m.mu.RLock()
	total := len(m.stoppedList)
	// aria2 对空结果返回 []（JSON 数组），而非 null
	if total == 0 {
		m.mu.RUnlock()
		return []TaskStatus{}
	}
	start, end, reversed := m.paginate(total, offset, num)
	if start >= end {
		m.mu.RUnlock()
		return []TaskStatus{}
	}

	result := make([]TaskStatus, 0, end-start)
	if reversed {
		for i := end - 1; i >= start; i-- {
			gid := m.stoppedList[i]
			if t, ok := m.tasks[gid]; ok {
				result = append(result, t.Snapshot())
			}
		}
	} else {
		for i := start; i < end; i++ {
			gid := m.stoppedList[i]
			if t, ok := m.tasks[gid]; ok {
				result = append(result, t.Snapshot())
			}
		}
	}
	m.mu.RUnlock()
	return result
}

// GlobalStat 全局统计
func (m *Manager) GlobalStat() GlobalStat {
	m.mu.RLock()
	active := m.activeCount
	waiting := 0
	stopped := len(m.stoppedList)
	for _, gid := range m.taskOrder {
		if t, ok := m.tasks[gid]; ok {
			s := t.Status()
			if s == StatusWaiting || s == StatusPaused {
				waiting++
			}
		}
	}
	m.mu.RUnlock()

	var totalSpeed int64
	for _, t := range m.TellActive() {
		if s, ok := parseSpeed(t.DownloadSpeed); ok {
			totalSpeed += s
		}
	}

	return GlobalStat{
		DownloadSpeed:   itoa(totalSpeed),
		UploadSpeed:     "0",
		NumActive:       itoa(int64(active)),
		NumWaiting:      itoa(int64(waiting)),
		NumStopped:      itoa(int64(stopped)),
		NumStoppedTotal: itoa(int64(stopped)),
	}
}

// ============================== 全局选项 ==============================

// GlobalOption 返回当前全局配置（aria2.getGlobalOption 兼容）。
func (m *Manager) GlobalOption() map[string]string {
	m.mu.RLock()
	limit := m.activeLimit
	workers := m.defaultWorker
	m.mu.RUnlock()
	return map[string]string{
		"max-concurrent-downloads":   itoa(int64(limit)),
		"max-connection-per-server":  itoa(int64(workers)),
		"split":                      itoa(int64(workers)),
		"max-overall-download-limit": "0",
		"dir":                        m.dir,
	}
}

// ChangeGlobalOption 修改全局配置（aria2.changeGlobalOption 兼容）。
func (m *Manager) ChangeGlobalOption(opts map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := opts["max-concurrent-downloads"]; ok {
		if n := toInt(v); n > 0 {
			m.activeLimit = n
		}
	}
	if v, ok := opts["max-connection-per-server"]; ok {
		if n := toInt(v); n > 0 {
			m.defaultWorker = n
		}
	}
	if v, ok := opts["split"]; ok {
		if n := toInt(v); n > 0 {
			m.defaultWorker = n
		}
	}
	if v, ok := opts["dir"]; ok {
		if d, ok := v.(string); ok && d != "" {
			m.dir = d
		}
	}
	return nil
}

// PurgeDownloadResult 批量清除所有已完成/错误/已删除的任务记录。
func (m *Manager) PurgeDownloadResult() {
	m.mu.Lock()
	defer m.mu.Unlock()

	toDelete := make([]string, 0)
	for gid, t := range m.tasks {
		s := t.Status()
		if s == StatusComplete || s == StatusError || s == StatusRemoved {
			toDelete = append(toDelete, gid)
		}
	}
	for _, gid := range toDelete {
		delete(m.tasks, gid)
	}
	// 清理 taskOrder
	filtered := make([]string, 0, len(m.taskOrder))
	for _, gid := range m.taskOrder {
		if _, ok := m.tasks[gid]; ok {
			filtered = append(filtered, gid)
		}
	}
	m.taskOrder = filtered
	m.stoppedList = m.stoppedList[:0]
}

// ============================== 单任务选项 / URI / Files ==============================

// GetOption 返回单个任务的选项（aria2.getOption 兼容）。
func (m *Manager) GetOption(gid string) (map[string]string, error) {
	t, err := m.getTask(gid)
	if err != nil {
		return nil, err
	}
	return t.GetOption(), nil
}

// ChangeOption 修改单个任务的选项（aria2.changeOption 兼容）。
func (m *Manager) ChangeOption(gid string, opts map[string]interface{}) error {
	t, err := m.getTask(gid)
	if err != nil {
		return err
	}
	return t.ChangeOption(opts)
}

// GetUris 返回任务关联的 URI 列表（aria2.getUris 兼容）。
func (m *Manager) GetUris(gid string) ([]URIInfo, error) {
	t, err := m.getTask(gid)
	if err != nil {
		return nil, err
	}
	return t.GetUris(), nil
}

// GetFiles 返回任务的文件列表（aria2.getFiles 兼容）。
func (m *Manager) GetFiles(gid string) ([]FileInfo, error) {
	t, err := m.getTask(gid)
	if err != nil {
		return nil, err
	}
	return t.GetFiles(), nil
}

// ChangeUri 增减任务关联的 URI（aria2.changeUri 兼容）。
// 本实现仅支持单文件（fileIndex==1）的 m3u8 下载，返回实际删除/新增数量。
func (m *Manager) ChangeUri(gid string, fileIndex int, delUris, addUris []string) (int, int, error) {
	if fileIndex != 1 {
		return 0, 0, fmt.Errorf("fileIndex is out of range")
	}
	t, err := m.getTask(gid)
	if err != nil {
		return 0, 0, err
	}
	return t.ChangeUri(delUris, addUris)
}

// GetPeers 返回任务的 BT peer 列表（aria2.getPeers 兼容）。
// 与 aria2 一致：任务存在时返回空列表（非 BT 任务无 peer）；任务不存在返回错误。
func (m *Manager) GetPeers(gid string) ([]interface{}, error) {
	if _, err := m.getTask(gid); err != nil {
		return nil, err
	}
	return []interface{}{}, nil
}

// GetServers 返回任务各文件的服务器连接信息（aria2.getServers 兼容）。
// 与 aria2 一致：仅 active 状态返回，否则返回错误。
// m3u8 下载由池化连接驱动，无法枚举每个镜像的独立连接，故 servers 为空数组。
func (m *Manager) GetServers(gid string) ([]ServersFileInfo, error) {
	t, err := m.getTask(gid)
	if err != nil {
		return nil, err
	}
	if t.Status() != StatusActive {
		return nil, fmt.Errorf("no active download for GID#%s", gid)
	}
	return []ServersFileInfo{{
		Index:   "1",
		Servers: []ServerInfo{},
	}}, nil
}

// ChangePosition 调整 waiting/paused 任务在等待队列中的位置（aria2.changePosition 兼容）。
// how ∈ POS_SET（绝对位置）/ POS_CUR（相对当前位置）/ POS_END（相对队尾，pos 通常为负数）。
// 返回移动后的新位置（0 基）。
func (m *Manager) ChangePosition(gid string, pos int, how string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.tasks[gid]
	if !ok {
		return 0, fmt.Errorf("task not found: %s", gid)
	}
	s := t.Status()
	if s != StatusWaiting && s != StatusPaused {
		return 0, fmt.Errorf("cannot change position: download %s is not in waiting/paused queue (status: %s)", gid, s)
	}

	// 收集 waiting+paused 的 GID 序列（按 taskOrder 保序）
	queue := make([]string, 0, len(m.taskOrder))
	for _, id := range m.taskOrder {
		if tt, ok := m.tasks[id]; ok {
			ss := tt.Status()
			if ss == StatusWaiting || ss == StatusPaused {
				queue = append(queue, id)
			}
		}
	}
	total := len(queue)
	if total == 0 {
		return 0, fmt.Errorf("waiting queue is empty")
	}

	cur := -1
	for i, id := range queue {
		if id == gid {
			cur = i
			break
		}
	}
	if cur < 0 {
		return 0, fmt.Errorf("GID#%s not found in the waiting queue", gid)
	}

	// 目标位置计算（与 aria2 IndexedList::move 边界语义一致）
	last := total - 1
	var dest int
	switch how {
	case "POS_SET":
		dest = pos
		if dest > last {
			dest = last
		}
	case "POS_CUR":
		dest = cur + pos
		if pos > 0 {
			if dest > last {
				dest = last
			}
		} else if dest < 0 {
			dest = 0
		}
	case "POS_END":
		dest = last + pos
		if dest > last {
			dest = last
		}
	default:
		return 0, fmt.Errorf("illegal how: %s", how)
	}
	if dest < 0 {
		dest = 0
	}
	if dest == cur {
		return dest, nil
	}

	// 从队列移除 gid 再插入 dest
	seq := make([]string, 0, total)
	seq = append(seq, queue[:cur]...)
	seq = append(seq, queue[cur+1:]...)
	seq = append(seq, "") // 扩容 1 格
	copy(seq[dest+1:], seq[dest:])
	seq[dest] = gid

	// 将新序列回写 taskOrder（仅替换 waiting/paused 槽位，其它任务原位不动）
	j := 0
	for i, id := range m.taskOrder {
		if tt, ok := m.tasks[id]; ok {
			ss := tt.Status()
			if ss == StatusWaiting || ss == StatusPaused {
				m.taskOrder[i] = seq[j]
				j++
			}
		}
	}
	return dest, nil
}

// ============================== 内部方法 ==============================

func (m *Manager) getTask(gid string) (*Task, error) {
	m.mu.RLock()
	t, ok := m.tasks[gid]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("task not found: %s", gid)
	}
	return t, nil
}

func (m *Manager) tryStartTask(t *Task) {
	m.mu.Lock()
	if m.activeCount >= m.activeLimit {
		m.mu.Unlock()
		return
	}
	if t.Status() != StatusWaiting {
		m.mu.Unlock()
		return
	}
	m.activeCount++
	m.mu.Unlock()

	m.startTask(t)
}

// tryStartNext 从等待队列中取一个任务启动。使用 Lock 确保 activeCount 原子递增，
// 避免多个 goroutine 同时绕过并发限制。
func (m *Manager) tryStartNext() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeCount >= m.activeLimit {
		return
	}

	for _, gid := range m.taskOrder {
		if t, ok := m.tasks[gid]; ok && t.Status() == StatusWaiting {
			m.activeCount++
			go m.startTask(t)
			return
		}
	}
}

// startTask 在已持有 activeCount 槽位的前提下启动任务（不再获取 m.mu）。
func (m *Manager) startTask(t *Task) {
	t.Start(func() {
		// 外部暂停/删除时 Manager 已处理槽位释放，跳过重复清理
		s := t.Status()
		if s == StatusPaused || s == StatusRemoved {
			return
		}
		m.mu.Lock()
		m.activeCount--
		m.mu.Unlock()
		m.pushStopped(t.GID)
		m.tryStartNext()
	})
}

// SetEventCallback 注册状态迁移事件回调（由 RPC 层注入，用于推送 aria2 通知）。
func (m *Manager) SetEventCallback(fn func(method, gid string)) {
	m.mu.Lock()
	m.eventFn = fn
	m.mu.Unlock()
}

// aria2Event 返回状态迁移对应的 aria2 通知方法名；无对应事件时返回空串。
// 映射依据 aria2 手册 Notifications 一节的语义。
func aria2Event(prev, next Status) string {
	switch next {
	case StatusActive:
		// 只有从等待队列进入下载才算「开始」，恢复下载不重复通知
		if prev == StatusWaiting {
			return "aria2.onDownloadStart"
		}
	case StatusPaused:
		if prev == StatusActive || prev == StatusWaiting {
			return "aria2.onDownloadPause"
		}
	case StatusRemoved:
		return "aria2.onDownloadStop"
	case StatusComplete:
		return "aria2.onDownloadComplete"
	case StatusError:
		return "aria2.onDownloadError"
	}
	return ""
}

func (m *Manager) onTaskUpdate(gid string, prev, next Status) {
	// 推送 aria2 通知（无 WS 连接时 Notify 自身为空操作）
	if method := aria2Event(prev, next); method != "" {
		m.mu.RLock()
		fn := m.eventFn
		m.mu.RUnlock()
		if fn != nil {
			fn(method, gid)
		}
	}

	// 防抖：最后一次状态变更 2 秒后才实际写入，避免高频更新启动大量 goroutine
	if m.sessionFile == "" {
		return
	}
	m.saveTimerMu.Lock()
	if m.saveTimer != nil {
		m.saveTimer.Reset(2 * time.Second)
	} else {
		m.saveTimer = time.AfterFunc(2*time.Second, func() {
			if err := m.SaveSession(m.sessionFile); err != nil {
				log.Printf("[session] save failed: %v", err)
			}
		})
	}
	m.saveTimerMu.Unlock()
}

func (m *Manager) pushStopped(gid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 去重：同一 GID 只应在 stoppedList 中出现一次，否则 tellStopped 会返回重复项
	for _, id := range m.stoppedList {
		if id == gid {
			return
		}
	}
	m.stoppedList = append(m.stoppedList, gid)
	if len(m.stoppedList) > m.stoppedMax {
		m.stoppedList = m.stoppedList[len(m.stoppedList)-m.stoppedMax:]
	}
}

func (m *Manager) collect(status Status) []TaskStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]TaskStatus, 0)
	for _, gid := range m.taskOrder {
		if t, ok := m.tasks[gid]; ok && t.Status() == status {
			result = append(result, t.Snapshot())
		}
	}
	return result
}

// ============================== 会话持久化 ==============================

// SaveSessionNow 立即将会话保存到配置的会话文件（aria2.saveSession 兼容）。
// 未配置 session-file 时返回错误（与 aria2 的 "Filename is not given." 一致）。
func (m *Manager) SaveSessionNow() error {
	m.mu.RLock()
	sf := m.sessionFile
	m.mu.RUnlock()
	if sf == "" {
		// 与 aria2 的 SaveSessionRpcMethod 文案保持一致
		return fmt.Errorf("Filename is not given.")
	}
	return m.SaveSession(sf)
}

// SaveSession 将当前活跃/等待/暂停任务写入会话文件。
// 使用原子写入（tmp + rename），并发安全。
func (m *Manager) SaveSession(path string) error {
	m.mu.RLock()
	entries := make([]SessionEntry, 0, len(m.tasks))
	for _, gid := range m.taskOrder {
		t, ok := m.tasks[gid]
		if !ok {
			continue
		}
		s := t.Status()
		if s == StatusComplete || s == StatusError || s == StatusRemoved {
			continue
		}
		entries = append(entries, t.SessionEntry())
	}
	m.mu.RUnlock()

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// LoadSession 从会话文件恢复任务列表。
// 恢复的任务设为 waiting，由 Manager 正常调度。
func (m *Manager) LoadSession(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read session: %w", err)
	}

	var entries []SessionEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("parse session: %w", err)
	}

	m.mu.Lock()
	for _, entry := range entries {
		if _, exists := m.tasks[entry.GID]; exists {
			continue
		}
		t := RestoreTask(entry, m.onTaskUpdate)
		m.tasks[t.GID] = t
		m.taskOrder = append(m.taskOrder, t.GID)
	}
	m.mu.Unlock()

	// 将所有等待任务推入调度
	for _, entry := range entries {
		t, err := m.getTask(entry.GID)
		if err != nil {
			continue
		}
		if t.Status() == StatusWaiting {
			m.tryStartTask(t)
		}
	}

	log.Printf("[session] 已从 %s 恢复 %d 个任务", path, len(entries))
	return nil
}

func parseSpeed(s string) (int64, bool) {
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err == nil
}
