package task

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)

// ============================== Manager ==============================

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

	sessionFile string // 会话文件路径（空=不保存）
}

// NewManager 创建管理器
// dir 为全局默认下载目录（空字符串 → 自动取 "."）。
// sessionFile 为空时跳过会话持久化。
func NewManager(maxConcurrent, defaultWorkers int, dir, sessionFile string) *Manager {
	if maxConcurrent <= 0 {
		maxConcurrent = 5
	}
	if defaultWorkers <= 0 {
		defaultWorkers = 3
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

	// 尝试立即启动
	m.tryStartTask(t)
	return t.GID, nil
}

// Remove 移除任务
func (m *Manager) Remove(gid string) error {
	t, err := m.getTask(gid)
	if err != nil {
		return err
	}

	wasActive := t.Status() == StatusActive
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
	for i, id := range m.taskOrder {
		if id == gid {
			m.taskOrder = append(m.taskOrder[:i], m.taskOrder[i+1:]...)
			break
		}
	}
	m.mu.Unlock()
	return nil
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

// PauseAll 暂停所有活跃任务
func (m *Manager) PauseAll() {
	m.mu.RLock()
	active := make([]*Task, 0)
	for _, gid := range m.taskOrder {
		if t, ok := m.tasks[gid]; ok && t.Status() == StatusActive {
			active = append(active, t)
		}
	}
	m.mu.RUnlock()

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

// TellWaiting 等待队列（aria2 兼容：包含 waiting 和 paused 任务）
func (m *Manager) TellWaiting(offset, num int) []TaskStatus {
	// 先 waiting 后 paused，保持插入顺序
	all := m.collect(StatusWaiting)
	all = append(all, m.collect(StatusPaused)...)
	total := len(all)
	if total == 0 {
		return nil
	}
	if offset < 0 {
		offset = total + offset
		if offset < 0 {
			offset = 0
		}
	}
	if offset >= total {
		return nil
	}
	if num <= 0 {
		num = total
	}
	end := offset + num
	if end > total {
		end = total
	}
	return all[offset:end]
}

// TellStopped 已完成/失败/已删除列表
func (m *Manager) TellStopped(offset, num int) []TaskStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 边界保护：aria2 用 -1 表示 "最后 num 条"
	total := len(m.stoppedList)
	if total == 0 {
		return nil
	}
	if offset < 0 {
		offset = total + offset
		if offset < 0 {
			offset = 0
		}
	}
	if offset >= total {
		return nil
	}
	if num <= 0 {
		num = total
	}
	end := offset + num
	if end > total {
		end = total
	}

	result := make([]TaskStatus, 0, end-offset)
	for i := offset; i < end; i++ {
		gid := m.stoppedList[i]
		if t, ok := m.tasks[gid]; ok {
			result = append(result, t.Snapshot())
		}
	}
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

func (m *Manager) onTaskUpdate() {
	// 状态变更时异步保存会话
	if m.sessionFile != "" {
		go m.SaveSession(m.sessionFile)
	}
}

func (m *Manager) pushStopped(gid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
