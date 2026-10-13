package dl

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestInflight_ReflectsRealConcurrency aria2 的 connections 字段应反映真实在途
// 请求数。这里播放列表只有 1 个分片、却配置 4 线程：不得上报 4。
func TestInflight_ReflectsRealConcurrency(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/index.m3u8", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n"+
			"#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:4.0,\nseg0.ts\n#EXT-X-ENDLIST\n")
	})
	mux.HandleFunc("/seg0.ts", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(250 * time.Millisecond) // 拉长窗口以便观测
		w.Write([]byte{0x47, 0x40, 0x00, 0x10})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	d := New(Config{
		M3U8URL: srv.URL + "/index.m3u8", OutputDir: t.TempDir(),
		MaxWorkers: 4, MaxRetry: 1, HostType: "v1", Quiet: true,
	})
	if err := d.Parse(); err != nil {
		t.Fatalf("parse: %v", err)
	}

	done := make(chan struct{})
	go func() {
		d.DownloadAll()
		close(done)
	}()

	maxSeen := 0
	running := true
	for running {
		select {
		case <-done:
			running = false
		default:
			if n := d.Inflight(); n > maxSeen {
				maxSeen = n
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	if maxSeen == 0 {
		t.Fatal("下载期间从未观测到在途请求（采样窗口过短？）")
	}
	if maxSeen != 1 {
		t.Fatalf("max Inflight = %d, want 1（只有 1 个分片，不得上报配置的线程数 4）", maxSeen)
	}
	if n := d.Inflight(); n != 0 {
		t.Fatalf("Inflight after done = %d, want 0", n)
	}
}
