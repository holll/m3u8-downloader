package task

import (
	"crypto/rand"
	"encoding/hex"
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
	return &Task{
		GID:        newGID(),
		status:     StatusWaiting,
		url:        url,
		dir:        opts.Dir,
		out:        opts.Out,
		cookie:     opts.Cookie,
		maxWorkers: n,
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

// --- 统计更新 ---

func (t *Task) addBytes(n int64) {
	atomic.AddInt64(&t.bytesReceived, n)
}

func (t *Task) addSegment() {
	atomic.AddInt64(&t.completedLength, 1)
}

func (t *Task) setSegmentTotal(n int64) {
	atomic.StoreInt64(&t.totalLength, n)
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
		defer func() {
			if onDone != nil {
				onDone()
			}
		}()

		// 创建下载器
		outDir := t.dir
		if outDir == "" {
			outDir = "."
		}
		outName := t.out
		if outName == "" {
			outName = t.GID
		}
		dlDir := filepath.Join(os.TempDir(), "m3u8_"+outName)
		if err := os.MkdirAll(dlDir, 0755); err != nil {
			t.setError("1", fmt.Sprintf("mkdir: %v", err))
			return
		}

		dl := getDL(t.url, dlDir, t.maxWorkers, t.cookie)

		// 注册回调
		dl.OnBytes(func(n int64) {
			t.addBytes(n)
		})
		dl.OnProgress(func(completed, total int64) {
			t.addSegment()
			t.setSegmentTotal(total)
		})

		// Phase 1: Parse
		if err := dl.Parse(); err != nil {
			t.setError("1", fmt.Sprintf("parse: %v", err))
			return
		}
		t.setSegmentTotal(dl.SegmentTotal())

		// 自动识别文件名（前置到下载前，AriaNg 可立即显示正确名称）
		if t.out == "" {
			dl.AutoName(outDir)
			t.out = strings.TrimSuffix(filepath.Base(dl.OutputFile()), ".mp4")
		}

		// 检查是否已暂停/取消
		if t.Status() == StatusPaused || t.Status() == StatusRemoved {
			return
		}

		// Phase 2: Download
		dl.DownloadAll()

		if t.Status() == StatusPaused || t.Status() == StatusRemoved {
			return
		}

		// Phase 3: Merge
		os.MkdirAll(outDir, 0755)
		dl.SetOutputFile(outDir + "/" + t.out + ".mp4")
		if _, err := dl.Merge(); err != nil {
			t.setError("2", fmt.Sprintf("merge: %v", err))
			return
		}

		// 清理临时目录
		os.RemoveAll(dlDir)

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

// getDL 创建 dl.Downloader 实例 (包级 helper)
func getDL(url, outputDir string, maxWorkers int, cookie string) *dl.Downloader {
	cfg := dl.Config{
		M3U8URL:    url,
		OutputDir:  outputDir,
		MaxWorkers: maxWorkers,
		HostType:   "v1",
		AutoClear:  false,
		AutoName:   true,
		Cookie:     cookie,
		Insecure:   false,
	}
	return dl.New(cfg)
}
