package task

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mustWaitingTasks 构造 n 个 waiting 任务直接注入 manager（同包测试，避免网络依赖）。
func mustWaitingTasks(m *Manager, n int) []string {
	gids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		t := NewTask(fmt.Sprintf("http://test/%d/index.m3u8", i), Options{}, nil)
		m.tasks[t.GID] = t
		m.taskOrder = append(m.taskOrder, t.GID)
		gids = append(gids, t.GID)
	}
	return gids
}

// waitingOrder 返回 manager 中 waiting/paused 的 GID 序列。
func waitingOrder(m *Manager) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var seq []string
	for _, id := range m.taskOrder {
		if tt, ok := m.tasks[id]; ok {
			s := tt.Status()
			if s == StatusWaiting || s == StatusPaused {
				seq = append(seq, id)
			}
		}
	}
	return seq
}

func TestChangePosition_POS_SET(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 4) // [g0 g1 g2 g3]

	// g0 移到队尾
	pos, err := m.ChangePosition(gids[0], 3, "POS_SET")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 3 {
		t.Fatalf("pos = %d, want 3", pos)
	}
	want := []string{gids[1], gids[2], gids[3], gids[0]}
	if got := waitingOrder(m); !eq(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}

	// 越界 clamp 到末尾
	pos, err = m.ChangePosition(gids[3], 99, "POS_SET")
	if err != nil || pos != 3 {
		t.Fatalf("pos=%d err=%v, want 3,nil", pos, err)
	}
}

func TestChangePosition_POS_CUR(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 4) // [g0 g1 g2 g3]

	// g0 相对 +2
	pos, err := m.ChangePosition(gids[0], 2, "POS_CUR")
	if err != nil || pos != 2 {
		t.Fatalf("pos=%d err=%v, want 2,nil", pos, err)
	}
	// 顺序 [g1 g2 g0 g3]
	want := []string{gids[1], gids[2], gids[0], gids[3]}
	if got := waitingOrder(m); !eq(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestChangePosition_POS_END(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 4) // [g0 g1 g2 g3]

	// g0 移到末尾（POS_END 且 pos=0 → 队尾；与 aria2 IndexedList::move 公式一致）
	pos, err := m.ChangePosition(gids[0], 0, "POS_END")
	if err != nil || pos != 3 {
		t.Fatalf("pos=%d err=%v, want 3,nil", pos, err)
	}
	want := []string{gids[1], gids[2], gids[3], gids[0]}
	if got := waitingOrder(m); !eq(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}

	// aria2 公式：dest = min(size-1+offset, size-1)；POS_END 且 pos=-1 → size-2
	m2 := NewManager(1, 3, ".", "")
	g2 := mustWaitingTasks(m2, 4)
	if pos, _ := m2.ChangePosition(g2[0], -1, "POS_END"); pos != 2 {
		t.Fatalf("pos=%d, want 2 (aria2 POS_END -1 → size-2)", pos)
	}
}

func TestChangePosition_Errors(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 2)

	// 非法 how
	if _, err := m.ChangePosition(gids[0], 0, "POS_BAD"); err == nil {
		t.Fatal("want error for illegal how")
	}
	// active 任务不可移动
	m.tasks[gids[1]].setStatusSilent(StatusActive)
	if _, err := m.ChangePosition(gids[1], 0, "POS_SET"); err == nil {
		t.Fatal("want error for active task")
	}
	// 不存在的 gid
	if _, err := m.ChangePosition("ffffffffffffffff", 0, "POS_SET"); err == nil {
		t.Fatal("want error for missing gid")
	}
}

func TestChangeUri(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)
	gid := gids[0]
	task := m.tasks[gid]

	// 删除当前 URL，提供新 URL → (1,1)，URL 被替换
	old := task.url
	del, add, err := m.ChangeUri(gid, 1, []string{old}, []string{"http://new/index.m3u8"})
	if err != nil || del != 1 || add != 1 {
		t.Fatalf("(del,add,err)=(%d,%d,%v), want (1,1,nil)", del, add, err)
	}
	if task.url != "http://new/index.m3u8" {
		t.Fatalf("url = %s, want replaced", task.url)
	}

	// fileIndex 越界
	if _, _, err := m.ChangeUri(gid, 2, nil, nil); err == nil {
		t.Fatal("want error for fileIndex out of range")
	}

	// active 状态禁止修改
	m.tasks[gid].setStatusSilent(StatusActive)
	if _, _, err := m.ChangeUri(gid, 1, nil, []string{"http://x.m3u8"}); err == nil {
		t.Fatal("want error for active task")
	}
}

func TestGetServers(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)

	// 非 active → 错误
	if _, err := m.GetServers(gids[0]); err == nil {
		t.Fatal("want error for non-active task")
	}
	// active → 单文件 + 空 servers
	m.tasks[gids[0]].setStatusSilent(StatusActive)
	sv, err := m.GetServers(gids[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sv) != 1 || sv[0].Index != "1" || len(sv[0].Servers) != 0 {
		t.Fatalf("unexpected servers: %+v", sv)
	}
}

func TestSaveSessionNow(t *testing.T) {
	// 未配置 session-file
	m := NewManager(1, 3, ".", "")
	if err := m.SaveSessionNow(); err == nil {
		t.Fatal("want error when session-file not configured")
	}

	// 配置 session-file（用 paused 任务，避免 LoadSession 恢复后自动启动真实下载 goroutine）
	path := filepath.Join(t.TempDir(), "test.session")
	m2 := NewManager(1, 3, ".", path)
	gids := mustWaitingTasks(m2, 1)
	m2.tasks[gids[0]].setStatusSilent(StatusPaused)
	if err := m2.SaveSessionNow(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("session file not written: %v", err)
	}

	// 恢复后任务仍在
	m3 := NewManager(1, 3, ".", path)
	if err := m3.LoadSession(path); err != nil {
		t.Fatalf("load session: %v", err)
	}
	if _, err := m3.getTask(gids[0]); err != nil {
		t.Fatalf("task not restored: %v", err)
	}
}

func TestTellStopped_AriaNgDefault(t *testing.T) {
	// AriaNg 默认调用 tellStopped(-1, 1000)：应返回全部、最新在前
	m := NewManager(5, 3, ".", "")
	// 注入 4 个 complete 任务并按完成顺序加入 stoppedList
	order := mustWaitingTasks(m, 4)
	m.taskOrder = nil // 清空，避免歧义；stoppedList 独立
	for _, g := range order {
		m.tasks[g].setStatusSilent(StatusComplete)
		m.pushStopped(g)
	}

	got := m.TellStopped(-1, 1000)
	if len(got) != 4 {
		t.Fatalf("len = %d, want 4", len(got))
	}
	// 最新在前：stoppedList=[o0 o1 o2 o3] → 返回 [o3 o2 o1 o0]
	want := []string{order[3], order[2], order[1], order[0]}
	for i, g := range got {
		if g.GID != want[i] {
			t.Fatalf("pos %d = %s, want %s", i, g.GID, want[i])
		}
	}

	// tellStopped(-1, 1) → 仅最新 1 条
	one := m.TellStopped(-1, 1)
	if len(one) != 1 || one[0].GID != order[3] {
		t.Fatalf("one = %+v, want last only", one)
	}

	// tellStopped(0, 1000) 正序 → 旧在前
	fwd := m.TellStopped(0, 1000)
	if len(fwd) != 4 || fwd[0].GID != order[0] || fwd[3].GID != order[3] {
		t.Fatalf("fwd order wrong: %v", []string{fwd[0].GID, fwd[3].GID})
	}

	// num=0 → 空（aria2 语义）
	if empty := m.TellStopped(0, 0); len(empty) != 0 {
		t.Fatalf("num=0 should be empty, got %d", len(empty))
	}
	// offset 越界 → 空
	if empty := m.TellStopped(100, 1000); len(empty) != 0 {
		t.Fatalf("offset overflow should be empty")
	}
}

func TestPlainTask_HasNoBittorrentKey(t *testing.T) {
	// 非 BT 任务快照不得输出 bittorrent 字段，否则 AriaNg 误判为 BT 下载并调用 getPeers。
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)
	m.tasks[gids[0]].setStatusSilent(StatusComplete)
	m.pushStopped(gids[0])

	list := m.TellStopped(0, 1)
	if len(list) != 1 {
		t.Fatalf("want 1 stopped task")
	}
	data, err := json.Marshal(list[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "bittorrent") {
		t.Fatalf("plain task snapshot must not contain bittorrent key: %s", data)
	}
}

func TestGetPeers(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)

	// 存在（非 BT）→ 空列表
	peers, err := m.GetPeers(gids[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if peers == nil || len(peers) != 0 {
		t.Fatalf("peers = %v, want empty list", peers)
	}

	// 不存在 → 错误
	if _, err := m.GetPeers("ffffffffffffffff"); err == nil {
		t.Fatal("want error for missing gid")
	}
}

func TestTaskStart_CreatesOutputDirFirst(t *testing.T) {
	base := t.TempDir()
	outDir := filepath.Join(base, "downloads", "sub")
	if _, err := os.Stat(outDir); err == nil {
		t.Fatal("precondition: outDir should not exist yet")
	}

	tk := NewTask("http://127.0.0.1:1/none.m3u8", Options{Dir: outDir, Out: "x"}, nil)
	done := make(chan struct{})
	tk.Start(func() { close(done) })

	// parse 会因连接拒绝快速失败，但输出目录必须在解析前就已创建
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not finish")
	}
	if tk.Status() != StatusError {
		t.Fatalf("status = %s, want error", tk.Status())
	}
	fi, err := os.Stat(outDir)
	if err != nil {
		t.Fatalf("output dir not created: %v", err)
	}
	if !fi.IsDir() {
		t.Fatalf("output path is not a dir: %v", outDir)
	}
}

// TestPushStopped_Dedup 同一 GID 重复入 stoppedList 会让 tellStopped 返回重复项，
// AriaNg 的已停止列表会因此出现重复行。
func TestPushStopped_Dedup(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)
	m.tasks[gids[0]].setStatusSilent(StatusComplete)

	m.pushStopped(gids[0])
	m.pushStopped(gids[0])

	if got := m.TellStopped(0, 100); len(got) != 1 {
		t.Fatalf("tellStopped len = %d, want 1 (duplicate suppressed)", len(got))
	}
}

// TestRemove_OnlyRunningGIDs 对齐 aria2：remove 仅作用于 active/waiting/paused，
// 已停止任务返回错误（aria2 文案为 "Active Download not found for GID#..."）。
func TestRemove_OnlyRunningGIDs(t *testing.T) {
	m := NewManager(5, 3, ".", "")
	gids := mustWaitingTasks(m, 3)

	// waiting → 可移除
	if err := m.Remove(gids[0]); err != nil {
		t.Fatalf("remove waiting: %v", err)
	}
	if m.tasks[gids[0]].Status() != StatusRemoved {
		t.Fatalf("status = %s, want removed", m.tasks[gids[0]].Status())
	}
	// 重复移除同一已停止任务 → 错误，且不产生重复项
	if err := m.Remove(gids[0]); err == nil {
		t.Fatal("want error when removing an already-removed task")
	}
	if got := m.TellStopped(0, 100); len(got) != 1 {
		t.Fatalf("tellStopped len = %d, want 1", len(got))
	}

	// complete/error 同样拒绝
	m.tasks[gids[1]].setStatusSilent(StatusComplete)
	m.pushStopped(gids[1])
	if err := m.Remove(gids[1]); err == nil {
		t.Fatal("want error when removing a completed task")
	}
	m.tasks[gids[2]].setStatusSilent(StatusError)
	if err := m.Remove(gids[2]); err == nil {
		t.Fatal("want error when removing an errored task")
	}

	// 不存在的 gid
	if err := m.Remove("ffffffffffffffff"); err == nil {
		t.Fatal("want error for missing gid")
	}
}

// TestPauseAll_IncludesWaiting aria2 的 pauseAll 同时暂停 active 与 waiting 队列。
func TestPauseAll_IncludesWaiting(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 2)
	m.tasks[gids[0]].setStatusSilent(StatusActive)
	m.activeCount = 1 // 模拟该任务占用下载槽位

	m.PauseAll()

	if s := m.tasks[gids[0]].Status(); s != StatusPaused {
		t.Fatalf("active task status = %s, want paused", s)
	}
	if s := m.tasks[gids[1]].Status(); s != StatusPaused {
		t.Fatalf("waiting task status = %s, want paused (aria2 pauseAll 也暂停等待队列)", s)
	}
	if m.activeCount != 0 {
		t.Fatalf("activeCount = %d, want 0", m.activeCount)
	}
}

// TestPauseAllThenUnpauseAll 暂停后 unpauseAll 应能让任务重新回到队列。
func TestPauseAllThenUnpauseAll(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 2)
	m.tasks[gids[0]].setStatusSilent(StatusActive)
	m.activeCount = 1

	m.PauseAll()
	// 断开网络依赖：unpause 后 tryStartTask 会真正启动下载，故仅校验状态机部分
	for _, g := range gids {
		m.tasks[g].setStatusSilent(StatusPaused)
	}
	for _, g := range gids {
		if err := m.tasks[g].Unpause(); err != nil {
			t.Fatalf("unpause %s: %v", g, err)
		}
		if s := m.tasks[g].Status(); s != StatusWaiting {
			t.Fatalf("status = %s, want waiting", s)
		}
	}
}

// TestChangeOption_ActiveAllowed aria2 允许对 active 下载调用 changeOption
// （作为 pending option）。AriaNg 的任务详情设置页对 active 任务同样可调用，
// 若拒绝则界面报错。
func TestChangeOption_ActiveAllowed(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)
	tk := m.tasks[gids[0]]
	tk.setStatusSilent(StatusActive)

	if err := m.ChangeOption(gids[0], map[string]interface{}{"max-download-limit": "100"}); err != nil {
		t.Fatalf("changeOption on active should be accepted: %v", err)
	}
	// 无效 gid 仍报错
	if err := m.ChangeOption("ffffffffffffffff", map[string]interface{}{}); err == nil {
		t.Fatal("want error for missing gid")
	}
	// 已停止任务拒绝（aria2 中已无对应 RequestGroup）
	tk.setStatusSilent(StatusComplete)
	if err := m.ChangeOption(gids[0], map[string]interface{}{}); err == nil {
		t.Fatal("want error for completed task")
	}
}

// TestAddURI_PauseOption AriaNg 的「添加到暂停队列」通过 addUri 的 pause 选项实现，
// 任务必须入队即暂停而不是立刻开始下载。
func TestAddURI_PauseOption(t *testing.T) {
	m := NewManager(5, 3, ".", "")
	gid, err := m.AddURI("http://127.0.0.1:1/x.m3u8", Options{Pause: true})
	if err != nil {
		t.Fatalf("addUri: %v", err)
	}
	if s := m.tasks[gid].Status(); s != StatusPaused {
		t.Fatalf("status = %s, want paused", s)
	}
	if m.activeCount != 0 {
		t.Fatalf("activeCount = %d, want 0 (paused task must not start)", m.activeCount)
	}
	// 暂停任务出现在 tellWaiting（aria2 语义：waiting 列表含 paused）
	if got := m.TellWaiting(0, 100); len(got) != 1 || got[0].GID != gid {
		t.Fatalf("tellWaiting = %+v, want the paused task", got)
	}
	// getOption 回传的 pause 选项应能再次被识别为暂停任务
	opts := OptionsFromMap(map[string]interface{}{"pause": "true"}, 3)
	if !opts.Pause {
		t.Fatal(`pause:"true" (AriaNg 的字符串写法) 应解析为 true`)
	}
	opts = OptionsFromMap(map[string]interface{}{"pause": true}, 3)
	if !opts.Pause {
		t.Fatal("pause:true (bool) 应解析为 true")
	}
	if OptionsFromMap(map[string]interface{}{}, 3).Pause {
		t.Fatal("缺省 pause 应为 false")
	}
}

// TestBitfieldHex 校验完成位图编码与 AriaNg 的 getPieceStatus 解码方式一致
// （每字符 4 个分片、高位在前）。
func TestBitfieldHex(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)
	tk := m.tasks[gids[0]]

	tk.setSegmentTotal(10)
	for _, i := range []int{0, 3, 4, 9} {
		tk.markSegmentDone(i)
	}
	// 长度 = ceil(n/4)；n=10 → 3 个字符
	if got := tk.bitfieldHex(10); got != "984" {
		t.Fatalf("bitfield = %s, want 984", got)
	}

	// 未完成的位图为全 0
	m2 := NewManager(1, 3, ".", "")
	g2 := mustWaitingTasks(m2, 1)
	m2.tasks[g2[0]].setSegmentTotal(5)
	if got := m2.tasks[g2[0]].bitfieldHex(5); got != "00" {
		t.Fatalf("bitfield = %s, want 00 (ceil(5/4)=2)", got)
	}

	// 位图按需扩容：先置位再设总数也不能丢（index 70 → 末字符第 3 位 = 2）
	m3 := NewManager(1, 3, ".", "")
	g3 := mustWaitingTasks(m3, 1)
	tk3 := m3.tasks[g3[0]]
	tk3.markSegmentDone(70) // 越界 → 自动扩容
	tk3.setSegmentTotal(72) // 72/4 = 18 个字符
	if got := tk3.bitfieldHex(72); len(got) != 18 || got[17] != '2' {
		t.Fatalf("bitfield = %s (len %d), want index 70 置位 (末字符 2)", got, len(got))
	}
}

// TestSnapshot_BitfieldReflectsProgress tellStatus 的 bitfield 需反映真实完成分片数。
func TestSnapshot_BitfieldReflectsProgress(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)
	tk := m.tasks[gids[0]]
	tk.setSegmentTotal(8)
	for i := 0; i < 8; i++ {
		tk.markSegmentDone(i)
	}
	snap := tk.Snapshot()
	if snap.NumPieces != "8" {
		t.Fatalf("numPieces = %s, want 8", snap.NumPieces)
	}
	// 8 个分片全部完成 → ceil(8/4)=2 个字符，全 f
	if snap.Bitfield != "ff" {
		t.Fatalf("bitfield = %s, want ff", snap.Bitfield)
	}
}

// TestListMethods_EmptyIsArray aria2 的 tellActive/tellWaiting/tellStopped 在无结果时
// 返回 JSON 数组 []，而非 null；返回 null 会让 AriaNg 的 extendArray 判空失败并
// 每次都退化为全量请求。
func TestListMethods_EmptyIsArray(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	cases := map[string]interface{}{
		"tellActive":  m.TellActive(),
		"tellWaiting": m.TellWaiting(0, 100),
		"tellStopped": m.TellStopped(-1, 1000),
	}
	for name, v := range cases {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("%s marshal: %v", name, err)
		}
		if string(data) != "[]" {
			t.Fatalf("%s = %s, want []", name, data)
		}
	}
}

// TestSnapshot_FollowedByEmptyArray aria2 对无后继任务的下载返回 []，不是 null。
func TestSnapshot_FollowedByEmptyArray(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)
	data, err := json.Marshal(m.tasks[gids[0]].Snapshot())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"followedBy":[]`) {
		t.Fatalf("followedBy 应为空数组: %s", data)
	}
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
