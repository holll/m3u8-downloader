package task

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
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
	url        string
	dir        string
	out        string
	cookie     string
	maxWorkers int
	maxRetry   int // 单分片重试次数

	// 持久化
	tempDir string // 下载临时目录路径（重启后恢复用）

	// 统计 (atomic)
	completedLength int64 // 已完成分段数 (供进度条百分比)
	totalLength     int64 // 总分段数
	bytesReceived   int64 // 实际下载字节数 (供速度计算)

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
	onUpdate func() // 通知 Manager 状态变更
	doneCh   chan struct{}
}

// NewTask 创建任务
func NewTask(url string, opts Options, onUpdate func()) *Task {
	n := opts.MaxWorkers
	if n <= 0 {
		n = 3
	}
	r := opts.MaxRetry
	if r <= 0 {
		r = 5
	}
	return &Task{
		GID:        newGID(),
		status:     StatusWaiting,
		url:        url,
		dir:        opts.Dir,
		out:        opts.Out,
		cookie:     opts.Cookie,
		maxWorkers: n,
		maxRetry:   r,
		createdAt:  now(),
		onUpdate:   onUpdate,
		doneCh:     make(chan struct{}),
	}
}

// --- 状态访问 ---

func (t *Task) Status() Status {
	t.statusMu.RLock()
	defer t.statusMu.RUnlock()
	return t.status
}

func (t *Task) setStatus(s Status) {
	t.statusMu.Lock()
	t.status = s
	t.statusMu.Unlock()
	if t.onUpdate != nil {
		t.onUpdate()
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
	// m3u8 可能更换过，cap 防止 completed > total
	t.capCompletedAtTotal()
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
		// 恢复时使用已保存的临时目录；否则基于 URL 哈希创建确定性目录（支持断点续传）
		dlDir = t.tempDir
		if dlDir == "" {
			hash := sha256.Sum256([]byte(t.url))
			dlDir = filepath.Join(os.TempDir(), "m3u8_"+hex.EncodeToString(hash[:8]))
		}
		t.tempDir = dlDir
		if err := os.MkdirAll(dlDir, 0755); err != nil {
			t.setError("1", fmt.Sprintf("mkdir: %v", err))
			return
		}

		dl := getDL(t.url, dlDir, t.maxWorkers, t.maxRetry, t.cookie)

		// 注册回调
		dl.OnBytes(func(n int64) {
			t.addBytes(n)
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
					os.RemoveAll(dlDir)
					os.Remove(outputPath + ".progress")
					t.setStatus(StatusComplete)
					t.finishedAt = now()
					return
				}
			}
			t.setError("1", fmt.Sprintf("parse: %v", err))
			return
		}
		t.setSegmentTotal(dl.SegmentTotal())

		// 自动识别文件名（前置到下载前，AriaNg 可立即显示正确名称）
		if t.out == "" {
			dl.AutoName(outDir)
			t.out = strings.TrimSuffix(filepath.Base(dl.OutputFile()), ".mp4")
		}
		outName = t.out // 同步局部变量，确保 progress 文件名正确

		// 输出文件已存在则跳过下载
		outputPath := filepath.Join(outDir, t.out+".mp4")
		if info, err := os.Stat(outputPath); err == nil && info.Size() > 0 {
			log.Printf("[task %s] 文件已存在，跳过: %s", t.GID, outputPath)
			t.setSegmentTotal(1)
			atomic.StoreInt64(&t.completedLength, 1)
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

		// Phase 3: Merge
		log.Printf("[task %s] 开始合并 → %s/%s.mp4", t.GID, outDir, t.out)
		os.MkdirAll(outDir, 0755)
		dl.SetOutputFile(outDir + "/" + t.out + ".mp4")
		if _, err := dl.Merge(); err != nil {
			t.setError("2", fmt.Sprintf("merge: %v", err))
			return
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

// --- 快照 (线程安全) ---

// Snapshot 返回当前状态的只读快照
func (t *Task) Snapshot() TaskStatus {
	s := t.Status()
	segDone := atomic.LoadInt64(&t.completedLength)
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

	// 进度：分段数 * 1MiB，确保 completedLength/totalLength 同量纲
	const segScale = 1 << 20 // 1 MiB
	displayTotal := segTotal * segScale
	displayDone := segDone * segScale
	if displayDone > displayTotal && displayTotal > 0 {
		displayDone = displayTotal
	}
	if s == StatusComplete {
		displayDone = displayTotal
	}

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
		Connections:     itoa(int64(t.maxWorkers)),
		ErrorCode:       t.errorCode,
		ErrorMessage:    t.errorMessage,
		Dir:             dir,
		NumPieces:       itoa(segTotal),
		PieceLength:     "1048576",
		InfoHash:        "",
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
func RestoreTask(entry SessionEntry, onUpdate func()) *Task {
	n := entry.MaxWorkers
	if n <= 0 {
		n = 3
	}
	t := &Task{
		GID:        entry.GID,
		url:        entry.URL,
		dir:        entry.Dir,
		out:        entry.Out,
		cookie:     entry.Cookie,
		maxWorkers: n,
		tempDir:    entry.TempDir,
		createdAt:  now(),
		onUpdate:   onUpdate,
		doneCh:     make(chan struct{}),
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
