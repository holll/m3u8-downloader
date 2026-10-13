package task

import (
	"sync"
	"testing"
)

// TestSetStatus_EmitsTransitions 状态迁移回调只在状态真正变化时触发，
// 且携带正确的 prev/next（WebSocket 通知依赖它区分 onDownloadStart/Pause/...）。
func TestSetStatus_EmitsTransitions(t *testing.T) {
	var mu sync.Mutex
	var got []string

	var tk *Task
	tk = NewTask("http://test/a.m3u8", Options{}, func(gid string, prev, next Status) {
		mu.Lock()
		defer mu.Unlock()
		if gid != tk.GID {
			t.Errorf("gid = %s, want %s", gid, tk.GID)
		}
		got = append(got, prev.String()+"→"+next.String())
	})

	tk.setStatus(StatusActive)        // waiting→active，触发
	tk.setStatus(StatusActive)        // 同状态，不触发
	tk.setStatus(StatusPaused)        // active→paused，触发
	tk.setStatusSilent(StatusWaiting) // 静默（恢复流程），不触发
	tk.setStatus(StatusComplete)      // waiting→complete，触发

	want := []string{"waiting→active", "active→paused", "waiting→complete"}
	if len(got) != len(want) {
		t.Fatalf("回调次数 = %d (%v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 次 = %s, want %s", i, got[i], want[i])
		}
	}
}

// TestAria2Event 迁移 → aria2 通知方法名的映射。
func TestAria2Event(t *testing.T) {
	cases := []struct {
		prev, next Status
		want       string
	}{
		{StatusWaiting, StatusActive, "aria2.onDownloadStart"},
		{StatusActive, StatusActive, ""}, // 同状态不产生事件
		{StatusPaused, StatusActive, ""}, // 恢复下载不算「开始」
		{StatusActive, StatusPaused, "aria2.onDownloadPause"},
		{StatusWaiting, StatusPaused, "aria2.onDownloadPause"},
		{StatusPaused, StatusPaused, ""},
		{StatusActive, StatusRemoved, "aria2.onDownloadStop"},
		{StatusWaiting, StatusRemoved, "aria2.onDownloadStop"},
		{StatusActive, StatusComplete, "aria2.onDownloadComplete"},
		{StatusActive, StatusError, "aria2.onDownloadError"},
		{StatusWaiting, StatusError, "aria2.onDownloadError"},
	}
	for _, c := range cases {
		if got := aria2Event(c.prev, c.next); got != c.want {
			t.Errorf("aria2Event(%s, %s) = %q, want %q", c.prev, c.next, got, c.want)
		}
	}
}

// TestManager_EventCallback 验证 Manager 会把迁移事件派发给注入的回调。
func TestManager_EventCallback(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	var mu sync.Mutex
	var events []string
	m.SetEventCallback(func(method, gid string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, method)
	})

	// 直接用 Manager 的回调构造任务，才能验证事件派发链路
	tk := NewTask("http://test/a.m3u8", Options{}, m.onTaskUpdate)
	m.mu.Lock()
	m.tasks[tk.GID] = tk
	m.taskOrder = append(m.taskOrder, tk.GID)
	m.mu.Unlock()

	tk.setStatus(StatusActive)
	tk.setStatus(StatusPaused)
	tk.setStatus(StatusComplete)

	mu.Lock()
	defer mu.Unlock()
	want := []string{"aria2.onDownloadStart", "aria2.onDownloadPause", "aria2.onDownloadComplete"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}
