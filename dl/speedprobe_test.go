package dl

import (
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestSpeedProbe 端到端验证并发数对真实下载耗时的影响。
// 用法: PROBE_M3U8=<m3u8地址> PROBE_N=16 PROBE_COUNT=32 go test ./dl/ -run TestSpeedProbe -v
func TestSpeedProbe(t *testing.T) {
	u := os.Getenv("PROBE_M3U8")
	if u == "" {
		t.Skip("PROBE_M3U8 未设置")
	}
	n, _ := strconv.Atoi(os.Getenv("PROBE_N")) // 0 = 走 dl 包默认并发（验证兜底不死锁）
	cnt, _ := strconv.Atoi(os.Getenv("PROBE_COUNT"))
	if cnt <= 0 {
		cnt = 32
	}

	d := New(Config{
		M3U8URL:    u,
		OutputDir:  t.TempDir(),
		MaxWorkers: n,
		HostType:   "v1",
		Quiet:      true,
		MaxRetry:   3,
	})
	if err := d.Parse(); err != nil {
		t.Fatalf("parse: %v", err)
	}
	startIdx, _ := strconv.Atoi(os.Getenv("PROBE_START"))
	total := d.SegmentCount()
	if startIdx >= total {
		startIdx = 0
	}
	if startIdx+cnt > total {
		cnt = total - startIdx
	}
	idx := make([]int, cnt)
	for i := range idx {
		idx[i] = startIdx + i
	}

	var bytes int64
	d.OnBytes(func(b int64) { atomic.AddInt64(&bytes, b) })

	t0 := time.Now()
	d.DownloadSelected(idx)
	el := time.Since(t0).Seconds()

	t.Logf("并发参数=%-3d 实际并发=%-3d 分片=[%d,%d) 共%-4d 耗时=%6.2fs 平均=%5.2fs/片 吞吐=%6.0f KB/s",
		n, d.maxWorkers, startIdx, startIdx+cnt, cnt, el, el/float64(cnt), float64(bytes)/1024/el)
}
