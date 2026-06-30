package task

import (
	"fmt"
	"sync"
)

// ============================== Manager ==============================

// Manager 任务管理器
type Manager struct {
	tasks         map[string]*Task
	mu            sync.RWMutex
	activeLimit   int
	activeCount   int
	stoppedList   []string // GID 列表 (已完成/失败/已删除，FIFO)
	stoppedMax    int      // stoppedList 最大长度
	defaultWorker int      // 单任务默认下载线程数
}

// NewManager 创建管理器
func NewManager(maxConcurrent, defaultWorkers int) *Manager {
	if maxConcurrent <= 0 {
		maxConcurrent = 5
	}
	if defaultWorkers <= 0 {
		defaultWorkers = 3
	}
	return &Manager{
		tasks:         make(map[string]*Task),
		activeLimit:   maxConcurrent,
		stoppedMax:    1000,
		defaultWorker: defaultWorkers,
	}
}

// DefaultWorkers 返回单任务默认线程数
func (m *Manager) DefaultWorkers() int { return m.defaultWorker }

// ============================== 任务操作 ==============================

// AddURI 添加一个下载任务
func (m *Manager) AddURI(url string, opts Options) (string, error) {
	if url == "" {
		return "", fmt.Errorf("URL is required")
	}

	t := NewTask(url, opts, m.onTaskUpdate)

	m.mu.Lock()
	m.tasks[t.GID] = t
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

// TellWaiting 等待队列
func (m *Manager) TellWaiting(offset, num int) []TaskStatus {
	all := m.collect(StatusWaiting)
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
	for _, t := range m.tasks {
		if t.Status() == StatusWaiting {
			waiting++
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

	t.Start(func() {
		// 下载完成后释放槽位
		m.mu.Lock()
		m.activeCount--
		m.mu.Unlock()
		m.pushStopped(t.GID)
		m.tryStartNext()
	})
}

func (m *Manager) tryStartNext() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, t := range m.tasks {
		if t.Status() == StatusWaiting {
			go m.tryStartTask(t)
			return
		}
	}
}

func (m *Manager) onTaskUpdate() {
	// 状态变更回调，当前仅用于触发统计刷新
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
	for _, t := range m.tasks {
		if t.Status() == status {
			result = append(result, t.Snapshot())
		}
	}
	return result
}
func parseSpeed(s string) (int64, bool) {
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err == nil
}
