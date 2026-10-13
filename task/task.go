package task

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"m3u8-downloader/dl"
)

// ============================== GID 生成 ==============================

func newGID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ============================== Task ==============================

// Task 单个下载任务
type Task struct {
	GID      string
	status   Status
	statusMu sync.RWMutex

	// 用户选项
	url         string
	dir         string
	out         string
	explicitOut bool // out 是否为调用方显式指定（显式时禁止 AutoName 覆盖）
	cookie      string
	maxWorkers  int
	maxRetry    int // 单分片重试次数

	// 持久化
	tempDir string // 下载临时目录路径（重启后恢复用）

	// 统计 (atomic)
	completedLength int64 // 已完成分段数 (供进度条百分比)
	totalLength     int64 // 总分段数
	bytesReceived   int64 // 实际下载字节数 (供速度计算)
	finalSize       int64 // 完成后产物的真实字节数（0 = 尚未完成）

	// 运行中的下载器，供 Snapshot 读取实时并发度（aria2 connections）
	dlRef atomic.Pointer[dl.Downloader]

	// 已完成分片位图（按 Segment.Index 置位），用于输出 aria2 的 bitfield
	segBitmap   []uint64
	segBitmapMu sync.Mutex

	// 速度计算 (1s 滑动窗口)
	speedMu         sync.Mutex
	speedSampleTime time.Time
	speedSampleB    int64
	cachedSpeed     int64

	// 错误信息
	errorMessage string
	errorCode    string

	// 时间戳
	createdAt  time.Time
	startedAt  time.Time
	finishedAt time.Time

	// 内部
	onUpdate UpdateFunc // 通知 Manager 状态变更
	doneCh   chan struct{}
}

// UpdateFunc 任务状态迁移回调（gid 与迁移前后的状态）。
// 仅在状态真正发生变化时触发（setStatusSilent 与恢复流程不触发）。
type UpdateFunc func(gid string, prev, next Status)

// NewTask 创建任务
func NewTask(rawURL string, opts Options, onUpdate UpdateFunc) *Task {
	n := opts.MaxWorkers
	if n <= 0 {
		n = DefaultMaxWorkers
	}
	r := opts.MaxRetry
	if r <= 0 {
		r = 5
	}

	// 默认文件名：优先取 URL fragment，其次取 path basename（去掉扩展名）
	out := opts.Out
	if out == "" {
		out = guessNameFromURL(rawURL)
	}

	return &Task{
		GID:         newGID(),
		status:      StatusWaiting,
		url:         rawURL,
		dir:         opts.Dir,
		out:         out,
		explicitOut: opts.Out != "",
		cookie:      opts.Cookie,
		maxWorkers:  n,
		maxRetry:    r,
		createdAt:   now(),
		onUpdate:    onUpdate,
		doneCh:      make(chan struct{}),
	}
}

// guessNameFromURL 从 URL 中提取默认文件名（不含扩展名）。
// 优先使用 fragment（如 #20260716_230000），其次使用 path 的 basename。
func guessNameFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err == nil && u.Fragment != "" {
		return u.Fragment
	}
	if err == nil && u.Path != "" {
		base := filepath.Base(u.Path)
		ext := filepath.Ext(base)
		if ext != "" {
			base = strings.TrimSuffix(base, ext)
		}
		if base != "" && base != "." {
			return base
		}
	}
	return ""
}

// --- 状态访问 ---

func (t *Task) Status() Status {
	t.statusMu.RLock()
	defer t.statusMu.RUnlock()
	return t.status
}

func (t *Task) setStatus(s Status) {
	t.statusMu.Lock()
	prev := t.status
	if prev == s {
		t.statusMu.Unlock()
		return
	}
	t.status = s
	t.statusMu.Unlock()
	if t.onUpdate != nil {
		t.onUpdate(t.GID, prev, s)
	}
}

// setStatusSilent 不触发 onUpdate 回调（用于会话恢复）
func (t *Task) setStatusSilent(s Status) {
	t.statusMu.Lock()
	t.status = s
	t.statusMu.Unlock()
}

// --- 统计更新 ---

func (t *Task) addBytes(n int64) {
	atomic.AddInt64(&t.bytesReceived, n)
}

func (t *Task) addSegment() {
	atomic.AddInt64(&t.completedLength, 1)
}

func (t *Task) setSegmentTotal(n int64) {
	atomic.StoreInt64(&t.totalLength, n)
	if n > 0 {
		t.resizeBitmap(n)
	}
	// m3u8 可能更换过，cap 防止 completed > total
	t.capCompletedAtTotal()
}

// resizeBitmap 按分片总数分配位图；总数变化（m3u8 更换）时重建。
func (t *Task) resizeBitmap(n int64) {
	words := int((n + 63) / 64)
	t.segBitmapMu.Lock()
	if len(t.segBitmap) != words {
		t.segBitmap = make([]uint64, words)
	}
	t.segBitmapMu.Unlock()
}

// markSegmentDone 记录某个分片已就绪（index 为 Segment.Index，0 基）。
// 位图按需扩容，因此与 setSegmentTotal 的调用先后顺序无关。
func (t *Task) markSegmentDone(index int) {
	if index < 0 {
		return
	}
	word := index / 64
	t.segBitmapMu.Lock()
	if word >= len(t.segBitmap) {
		grown := make([]uint64, word+1)
		copy(grown, t.segBitmap)
		t.segBitmap = grown
	}
	t.segBitmap[word] |= 1 << uint(index%64)
	t.segBitmapMu.Unlock()
}

// bitfieldHex 生成 aria2 风格的完成位图十六进制串。
// 每个十六进制字符承载 4 个分片，高位对应靠前的分片 —— 与 AriaNg 的
// aria2TaskService.getPieceStatus 解析方式一致。
func (t *Task) bitfieldHex(n int64) string {
	if n <= 0 {
		return ""
	}
	t.segBitmapMu.Lock()
	defer t.segBitmapMu.Unlock()

	buf := make([]byte, 0, (n+3)/4)
	for base := int64(0); base < n; base += 4 {
		var v byte
		for i := int64(0); i < 4; i++ {
			v <<= 1
			idx := base + i
			if idx >= n {
				continue
			}
			word := idx / 64
			if word < int64(len(t.segBitmap)) && t.segBitmap[word]&(1<<uint(idx%64)) != 0 {
				v |= 1
			}
		}
		buf = append(buf, "0123456789abcdef"[v])
	}
	return string(buf)
}

// capCompletedAtTotal 确保 completedLength ≤ totalLength
func (t *Task) capCompletedAtTotal() {
	total := atomic.LoadInt64(&t.totalLength)
	for {
		completed := atomic.LoadInt64(&t.completedLength)
		if completed <= total {
			return
		}
		if atomic.CompareAndSwapInt64(&t.completedLength, completed, total) {
			return
		}
	}
}

func (t *Task) setError(code, msg string) {
	t.errorCode = code
	t.errorMessage = msg
	log.Printf("[task %s] error: %s", t.GID, msg)
	t.setStatus(StatusError)
	t.finishedAt = now()
	close(t.doneCh)
}

// --- 控制方法 ---

// Pause 暂停任务
func (t *Task) Pause() error {
	s := t.Status()
	if s != StatusWaiting && s != StatusActive {
		return fmt.Errorf("task %s cannot be paused (status: %s)", t.GID, s)
	}
	t.setStatus(StatusPaused)
	return nil
}

// Unpause 恢复暂停的任务
func (t *Task) Unpause() error {
	if t.Status() != StatusPaused {
		return fmt.Errorf("task %s is not paused", t.GID)
	}
	t.setStatus(StatusWaiting) // 回到等待队列，由 Manager 调度
	return nil
}

// Remove 移除任务
func (t *Task) Remove() {
	s := t.Status()
	if s == StatusRemoved {
		return
	}
	t.setStatus(StatusRemoved)
	t.finishedAt = now()
	select {
	case <-t.doneCh:
	default:
		close(t.doneCh)
	}
}

// --- 执行 (接入 dl.Downloader) ---

// Start 在独立 goroutine 中启动下载
func (t *Task) Start(onDone func()) {
	t.startedAt = now()
	t.speedSampleTime = t.startedAt
	t.setStatus(StatusActive)

	go func() {
		var dlDir string   // 提前声明，供 defer 闭包捕获
		var outDir string  // ditto
		var outName string // ditto

		defer func() {
			// 异常退出时保存进度（暂停/错误）
			t.trySaveProgress(outDir, outName)
			if onDone != nil {
				onDone()
			}
		}()

		// 创建下载器
		outDir = t.dir
		if outDir == "" {
			outDir = "."
		}
		outName = t.out
		if outName == "" {
			outName = t.GID
		}

		// 优先确保下载输出目录存在（AriaNg 指定的 dir 可能尚未创建），
		// 再创建下载临时目录，最后才解析 m3u8。
		if err := os.MkdirAll(outDir, 0755); err != nil {
			t.setError(aria2ErrorCode(err), fmt.Sprintf("mkdir output dir: %v", err))
			return
		}

		// 恢复时使用已保存的临时目录；否则基于 URL 哈希创建确定性目录（支持断点续传）
		dlDir = t.tempDir
		if dlDir == "" {
			hash := sha256.Sum256([]byte(t.url))
			dlDir = filepath.Join(os.TempDir(), "m3u8_"+hex.EncodeToString(hash[:8]))
		}
		t.tempDir = dlDir
		if err := os.MkdirAll(dlDir, 0755); err != nil {
			t.setError(aria2ErrorCode(err), fmt.Sprintf("mkdir: %v", err))
			return
		}

		dl := getDL(t.url, dlDir, t.maxWorkers, t.maxRetry, t.cookie)
		t.dlRef.Store(dl) // 供 Snapshot 上报实时并发度

		// 注册回调
		dl.OnBytes(func(n int64) {
			t.addBytes(n)
		})
		dl.OnSegmentDone(func(index int) {
			t.markSegmentDone(index)
		})
		dl.OnProgress(func(completed, total int64) {
			t.addSegment()
			t.setSegmentTotal(total)
			// 每 20 个切片写一次进度文件（输出目录下 xxx.mp4.progress）
			if completed > 0 && completed%20 == 0 {
				t.saveProgress(outDir, outName)
			}
		})

		// Phase 1: Parse
		log.Printf("[task %s] 开始解析 %s", t.GID, t.url)
		if err := dl.Parse(); err != nil {
			// 解析失败但输出文件已知且已存在 → 视为已完成
			if t.out != "" {
				outputPath := filepath.Join(outDir, t.out+".mp4")
				if info, statErr := os.Stat(outputPath); statErr == nil && info.Size() > 0 {
					log.Printf("[task %s] 文件已存在，跳过: %s", t.GID, outputPath)
					t.setSegmentTotal(1)
					atomic.StoreInt64(&t.completedLength, 1)
					t.markSegmentDone(0) // 与 numPieces=1 保持自洽
					atomic.StoreInt64(&t.finalSize, info.Size())
					os.RemoveAll(dlDir)
					os.Remove(outputPath + ".progress")
					t.setStatus(StatusComplete)
					t.finishedAt = now()
					return
				}
			}
			t.setError(aria2ErrorCode(err), fmt.Sprintf("parse: %v", err))
			return
		}
		t.setSegmentTotal(dl.SegmentTotal())

		// 自动命名：仅当用户未显式指定 out 且流含 PROGRAM-DATE-TIME 时，
		// 用时间戳名覆盖 URL basename 猜测值；显式 out 一律尊重。
		dl.AutoName(outDir)
		if !t.explicitOut && dl.AutoNamed() {
			if name := strings.TrimSuffix(filepath.Base(dl.OutputFile()), ".mp4"); name != "" {
				t.out = name
			}
		}
		outName = t.out // 同步局部变量，确保 progress 文件名正确
		// 输出文件已存在则跳过下载
		outputPath := filepath.Join(outDir, t.out+".mp4")
		if info, err := os.Stat(outputPath); err == nil && info.Size() > 0 {
			log.Printf("[task %s] 文件已存在，跳过: %s", t.GID, outputPath)
			t.setSegmentTotal(1)
			atomic.StoreInt64(&t.completedLength, 1)
			t.markSegmentDone(0) // 与 numPieces=1 保持自洽
			atomic.StoreInt64(&t.finalSize, info.Size())
			os.RemoveAll(dlDir)                 // 清理可能残留的临时目录
			os.Remove(outputPath + ".progress") // 清理进度文件
			t.setStatus(StatusComplete)
			t.finishedAt = now()
			return
		}

		// 检查是否已暂停/取消
		if t.Status() == StatusPaused || t.Status() == StatusRemoved {
			return
		}

		// Phase 2: Download
		log.Printf("[task %s] 开始下载 %d 个分片 (%d 线程)", t.GID, dl.SegmentCount(), t.maxWorkers)
		dl.DownloadAll()
		log.Printf("[task %s] 分片下载完成", t.GID)

		if t.Status() == StatusPaused || t.Status() == StatusRemoved {
			return
		}

		// 任一分片永久失败时不得继续合并：Merge 只会把现存分片拼接起来并返回成功，
		// 产出缺少片段的视频。此处以上报真实原因终止任务，避免静默产出残缺文件。
		if err := dl.SegmentError(); err != nil {
			t.setError(aria2ErrorCode(err), fmt.Sprintf("download: %v", err))
			return
		}

		// Phase 3: Merge
		log.Printf("[task %s] 开始合并 → %s/%s.mp4", t.GID, outDir, t.out)
		os.MkdirAll(outDir, 0755)
		dl.SetOutputFile(outDir + "/" + t.out + ".mp4")
		mv, err := dl.Merge()
		if err != nil {
			t.setError(aria2ErrorCode(err), fmt.Sprintf("merge: %v", err))
			return
		}
		// 完成后以真实文件大小上报容量，避免合成量纲与用户看到的文件不符
		if info, statErr := os.Stat(mv); statErr == nil {
			atomic.StoreInt64(&t.finalSize, info.Size())
		}
		log.Printf("[task %s] 合并完成", t.GID)

		// 清理临时目录 & 进度文件
		os.RemoveAll(dlDir)
		os.Remove(filepath.Join(outDir, t.out+".mp4.progress"))

		log.Printf("[task %s] complete: %s/%s.mp4", t.GID, outDir, t.out)
		t.setStatus(StatusComplete)
		t.finishedAt = now()
	}()
}

// --- 上报字段计算 ---

// progressBytes 返回上报给 aria2 的 totalLength / completedLength。
//
// m3u8 无法预知总字节数，下载中沿用「分片数 × 1MiB」作为同量纲占位值以驱动
// 进度比（ratio 精确等于分片完成比例）；任务完成后改用产物文件的真实大小，
// 使客户端的容量显示与实际文件一致。
func (t *Task) progressBytes() (total, done int64) {
	if size := atomic.LoadInt64(&t.finalSize); size > 0 {
		return size, size
	}

	const segScale = 1 << 20 // 1 MiB
	total = atomic.LoadInt64(&t.totalLength) * segScale
	done = atomic.LoadInt64(&t.completedLength) * segScale
	if done > total && total > 0 {
		done = total
	}
	return total, done
}

// connections 返回当前实际并发数（aria2 的 connections 字段）。
// 无运行中的下载器（如等待/已恢复未启动）时为 0。
func (t *Task) connections() int64 {
	if p := t.dlRef.Load(); p != nil {
		return int64(p.Inflight())
	}
	return 0
}

// pieceLength 返回单片字节数，使 numPieces × pieceLength ≈ totalLength 自洽
// （aria2 的 pieceLength 与 totalLength/numPieces 一致）。
func pieceLength(total, pieces int64) int64 {
	if pieces <= 0 || total <= 0 {
		return 0
	}
	return total / pieces
}

// --- 快照 (线程安全) ---

// Snapshot 返回当前状态的只读快照
func (t *Task) Snapshot() TaskStatus {
	s := t.Status()
	segTotal := atomic.LoadInt64(&t.totalLength)
	bytes := atomic.LoadInt64(&t.bytesReceived)

	// 下载速度：1s 滑动窗口瞬时速度
	speed := int64(0)
	if s == StatusActive {
		t.speedMu.Lock()
		elapsed := time.Since(t.speedSampleTime)
		if elapsed >= time.Second {
			db := bytes - t.speedSampleB
			if db > 0 && elapsed > 0 {
				t.cachedSpeed = int64(float64(db) / elapsed.Seconds())
			}
			t.speedSampleTime = time.Now()
			t.speedSampleB = bytes
		}
		speed = t.cachedSpeed
		t.speedMu.Unlock()
	}

	displayTotal, displayDone := t.progressBytes()

	dir := t.dir
	out := t.out
	if out == "" {
		out = t.GID
	}
	outPath := dir
	if outPath != "" && out != "" {
		outPath = outPath + "/" + out + ".mp4"
	}

	ts := TaskStatus{
		GID:             t.GID,
		Status:          s.String(),
		TotalLength:     itoa(displayTotal),
		CompletedLength: itoa(displayDone),
		DownloadSpeed:   itoa(speed),
		UploadSpeed:     "0",
		UploadLength:    "0",
		Connections:     itoa(t.connections()),
		ErrorCode:       t.errorCode,
		ErrorMessage:    t.errorMessage,
		Dir:             dir,
		NumPieces:       itoa(segTotal),
		PieceLength:     itoa(pieceLength(displayTotal, segTotal)),
		InfoHash:        "",
		// aria2 对无后继任务的下载返回空数组而非 null
		FollowedBy: []string{},
		Bitfield:   t.bitfieldHex(segTotal),
	}

	if outPath != "" {
		ts.Files = []FileInfo{{
			Index:           "1",
			Path:            outPath,
			Length:          itoa(displayTotal),
			CompletedLength: itoa(displayDone),
			Selected:        "true",
			URIs: []URIInfo{{
				URI:    t.url,
				Status: "used",
			}},
		}}
	}

	return ts
}

// ============================== 选项查询/修改 ==============================

// GetOption 返回任务的配置选项（aria2 兼容格式）。
func (t *Task) GetOption() map[string]string {
	return map[string]string{
		"dir":                       t.dir,
		"out":                       t.out,
		"max-connection-per-server": itoa(int64(t.maxWorkers)),
		"split":                     itoa(int64(t.maxWorkers)),
	}
}

// ChangeOption 修改任务的配置选项。
// active 状态同样接受（aria2 会将其作为 pending option，暂停后生效），
// 否则 AriaNg 在下载中修改选项会报错；已停止（complete/error/removed）的任务
// 在 aria2 中已无对应 RequestGroup，故一并拒绝。
func (t *Task) ChangeOption(opts map[string]interface{}) error {
	s := t.Status()
	if s == StatusComplete || s == StatusError || s == StatusRemoved {
		return fmt.Errorf("cannot change option in status: %s", s)
	}
	if v, ok := opts["dir"]; ok {
		if dir, ok := v.(string); ok && dir != "" {
			t.dir = dir
		}
	}
	if v, ok := opts["max-connection-per-server"]; ok {
		t.maxWorkers = toInt(v)
	}
	if v, ok := opts["split"]; ok {
		t.maxWorkers = toInt(v)
	}
	return nil
}

// GetUris 返回任务关联的 URI 列表。
func (t *Task) GetUris() []URIInfo {
	return []URIInfo{{URI: t.url, Status: "used"}}
}

// ChangeUri 增减下载 URI（aria2.changeUri 兼容，仅 waiting/paused 状态可修改）。
// m3u8 为单 URL 下载：若删除当前 URI 且提供了新 URI，则以首个新 URI 取代主 URL；
// 不维护多镜像列表。返回 (删除数, 新增数)。
//
// 与 aria2 的已知差异：aria2 对任意状态的下载都允许改 URI（运行中会切换到新源）。
// 本实现的 HLS 在 Parse 阶段已把全部分片 URL 解析完毕，运行中改主 URL 不产生任何
// 实际效果，因此对 active 明确拒绝，而不是返回 OK 却无作用。
// 错误文案与 aria2 的 ChangeUriRpcMethod 保持一致，便于客户端复用既有提示。
func (t *Task) ChangeUri(delUris, addUris []string) (int, int, error) {
	s := t.Status()
	if s != StatusWaiting && s != StatusPaused {
		return 0, 0, fmt.Errorf("Cannot remove URIs from GID#%s", t.GID)
	}

	del := 0
	for _, u := range delUris {
		if u == t.url {
			t.url = ""
			del = 1
			break
		}
	}

	add := 0
	if t.url == "" {
		for _, u := range addUris {
			if u == "" {
				continue
			}
			t.url = u
			add = 1
			break
		}
	}

	if t.url == "" {
		return 0, 0, fmt.Errorf("no usable URI left")
	}
	return del, add, nil
}

// GetFiles 返回任务的文件列表。
func (t *Task) GetFiles() []FileInfo {
	dir := t.dir
	out := t.out
	if out == "" {
		out = t.GID
	}
	outPath := dir
	if outPath != "" && out != "" {
		outPath = outPath + "/" + out + ".mp4"
	}
	displayTotal, displayDone := t.progressBytes()
	return []FileInfo{{
		Index:           "1",
		Path:            outPath,
		Length:          itoa(displayTotal),
		CompletedLength: itoa(displayDone),
		Selected:        "true",
		URIs: []URIInfo{{
			URI:    t.url,
			Status: "used",
		}},
	}}
}

// getDL 创建 dl.Downloader 实例 (包级 helper, Server 模式静默)
func getDL(url, outputDir string, maxWorkers, maxRetry int, cookie string) *dl.Downloader {
	cfg := dl.Config{
		M3U8URL:    url,
		OutputDir:  outputDir,
		MaxWorkers: maxWorkers,
		HostType:   "v1",
		AutoClear:  false,
		AutoName:   true,
		Cookie:     cookie,
		Insecure:   false,
		Quiet:      true, // Server 模式不打印进度条
		MaxRetry:   maxRetry,
	}
	return dl.New(cfg)
}

// ============================== 会话持久化 ==============================

// SessionEntry 导出任务当前状态，供会话文件保存。
func (t *Task) SessionEntry() SessionEntry {
	return SessionEntry{
		GID:               t.GID,
		URL:               t.url,
		Dir:               t.dir,
		Out:               t.out,
		Cookie:            t.cookie,
		MaxWorkers:        t.maxWorkers,
		Status:            t.Status().String(),
		TempDir:           t.tempDir,
		TotalSegments:     atomic.LoadInt64(&t.totalLength),
		CompletedSegments: atomic.LoadInt64(&t.completedLength),
		BytesReceived:     atomic.LoadInt64(&t.bytesReceived),
	}
}

// RestoreTask 从会话条目恢复任务（不触发 onUpdate）。
func RestoreTask(entry SessionEntry, onUpdate UpdateFunc) *Task {
	n := entry.MaxWorkers
	if n <= 0 {
		n = DefaultMaxWorkers
	}
	t := &Task{
		GID:         entry.GID,
		url:         entry.URL,
		dir:         entry.Dir,
		out:         entry.Out,
		explicitOut: entry.Out != "",
		cookie:      entry.Cookie,
		maxWorkers:  n,
		tempDir:     entry.TempDir,
		createdAt:   now(),
		onUpdate:    onUpdate,
		doneCh:      make(chan struct{}),
	}
	atomic.StoreInt64(&t.totalLength, entry.TotalSegments)
	atomic.StoreInt64(&t.completedLength, entry.CompletedSegments)
	atomic.StoreInt64(&t.bytesReceived, entry.BytesReceived)

	// 如果输出目录下有 .progress 文件，用其中的进度覆盖（更精确）
	if entry.Dir != "" && entry.Out != "" {
		if pf, err := loadProgress(entry.Dir, entry.Out); err == nil {
			atomic.StoreInt64(&t.totalLength, pf.TotalSegments)
			atomic.StoreInt64(&t.completedLength, pf.CompletedSegments)
			atomic.StoreInt64(&t.bytesReceived, pf.BytesReceived)
		}
	}

	// 恢复后统一设为 waiting，由 Manager 调度
	switch entry.Status {
	case "paused":
		t.setStatusSilent(StatusPaused)
	default:
		t.setStatusSilent(StatusWaiting)
	}
	return t
}

// ============================== 进度文件 (xxx.mp4.progress) ==============================

// saveProgress 将当前下载进度写入输出目录，文件名为 {out}.mp4.progress。
// 例如 /data/1.mp4 → /data/1.mp4.progress
func (t *Task) saveProgress(outDir, out string) {
	total := atomic.LoadInt64(&t.totalLength)
	completed := atomic.LoadInt64(&t.completedLength)
	if completed > total {
		completed = total
	}
	pf := ProgressFile{
		GID:               t.GID,
		TotalSegments:     total,
		CompletedSegments: completed,
		BytesReceived:     atomic.LoadInt64(&t.bytesReceived),
		UpdatedAt:         now(),
	}
	data, err := json.Marshal(pf)
	if err != nil {
		return
	}
	path := filepath.Join(outDir, out+".mp4.progress")
	os.WriteFile(path, data, 0644)
}

// trySaveProgress 仅在暂停/错误状态下保存进度。
func (t *Task) trySaveProgress(outDir, out string) {
	s := t.Status()
	if s == StatusPaused || s == StatusError {
		t.saveProgress(outDir, out)
	}
}

// loadProgress 从 {outDir}/{out}.mp4.progress 读取进度。
func loadProgress(outDir, out string) (*ProgressFile, error) {
	path := filepath.Join(outDir, out+".mp4.progress")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pf ProgressFile
	if err := json.Unmarshal(data, &pf); err != nil {
		return nil, err
	}
	return &pf, nil
}
