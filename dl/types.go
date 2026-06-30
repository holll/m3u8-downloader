// Package dl 提供 HLS (m3u8) 视频流的下载、解密和合并功能。
package dl

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ============================== 导出常量 ==============================

const (
	DefaultTimeout = 30e9 // 30s (nanoseconds, 供外部使用)
	ProgressWidth  = 20
)

// ============================== 导出类型 ==============================

// KeyInfo AES-128 解密密钥信息
type KeyInfo struct {
	URI string
	IV  []byte // nil 时按 HLS 规范使用 segment 序列号
}

// MapInfo fMP4 init segment 信息 (EXT-X-MAP)
type MapInfo struct {
	URI    string
	Limit  int64
	Offset int64
}

// Segment 单个切片信息
type Segment struct {
	URL             string
	Key             *KeyInfo
	Map             *MapInfo
	Index           int
	Duration        float64
	ProgramDateTime time.Time // EXT-X-PROGRAM-DATE-TIME (用于自动识别文件名)
}

// ProgressTracker 线程安全的进度跟踪器
type ProgressTracker struct {
	completed int64 // atomic
	failed    int64 // atomic
	total     int64
	mu        sync.Mutex
}

// Done 标记一个切片下载完成
func (p *ProgressTracker) Done() {
	atomic.AddInt64(&p.completed, 1)
}

// Fail 标记一个切片下载失败
func (p *ProgressTracker) Fail() {
	atomic.AddInt64(&p.failed, 1)
}

// Draw 绘制进度条到 stdout（单 goroutine 调用）
func (p *ProgressTracker) Draw(prefix string, width int) {
	done := atomic.LoadInt64(&p.completed)
	failed := atomic.LoadInt64(&p.failed)
	total := p.total
	ok := done - failed // 实际成功数

	ratio := float32(done) / float32(total)
	if ratio > 1.0 {
		ratio = 1.0
	}
	pos := int(ratio * float32(width))
	s := fmt.Sprintf("[%s] %s%*s %6.2f%% (%d/%d, err:%d)",
		prefix, strings.Repeat("■", pos), width-pos, "", ratio*100, ok, total, failed)
	fmt.Print("\r" + s)
	os.Stdout.Sync()
}
