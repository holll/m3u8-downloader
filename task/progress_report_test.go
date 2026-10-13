package task

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

const segPayloadSize = 4096

// startPlaylistServer 提供一个 2 分片的 m3u8 源，每片 segPayloadSize 字节。
func startPlaylistServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/index.m3u8", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n"+
			"#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:4.0,\nseg0.ts\n#EXTINF:4.0,\nseg1.ts\n#EXT-X-ENDLIST\n")
	})
	for _, name := range []string{"/seg0.ts", "/seg1.ts"} {
		mux.HandleFunc(name, func(w http.ResponseWriter, r *http.Request) {
			body := make([]byte, segPayloadSize)
			body[0] = 0x47 // TS 同步字节，避免被 stripBeforeSyncByte 截断
			w.Write(body)
		})
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func runTaskToEnd(t *testing.T, tk *Task) {
	t.Helper()
	done := make(chan struct{})
	tk.Start(func() { close(done) })
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("task did not finish")
	}
}

// TestSnapshot_CompletedReportsRealFileSize 完成后 totalLength/completedLength
// 必须是产物文件的真实字节数，而不是合成的量纲占位值。
func TestSnapshot_CompletedReportsRealFileSize(t *testing.T) {
	srv := startPlaylistServer(t)
	outDir := t.TempDir()

	tk := NewTask(srv.URL+"/index.m3u8", Options{Dir: outDir, Out: "size", MaxWorkers: 2}, nil)
	runTaskToEnd(t, tk)

	if s := tk.Status(); s != StatusComplete {
		t.Fatalf("status = %s, want complete", s)
	}

	outPath := filepath.Join(outDir, "size.mp4")
	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("产物不存在: %v", err)
	}
	wantSize := strconv.FormatInt(info.Size(), 10)
	// 两个分片各 4096 字节，合并后应为 8192（数据以 0x47 开头，不会被裁剪）
	if info.Size() != 2*segPayloadSize {
		t.Fatalf("产物大小 = %d, want %d", info.Size(), 2*segPayloadSize)
	}

	snap := tk.Snapshot()
	if snap.TotalLength != wantSize {
		t.Fatalf("totalLength = %s, want 真实文件大小 %s", snap.TotalLength, wantSize)
	}
	if snap.CompletedLength != wantSize {
		t.Fatalf("completedLength = %s, want %s", snap.CompletedLength, wantSize)
	}
	if len(snap.Files) != 1 || snap.Files[0].Length != wantSize {
		t.Fatalf("files[0].length = %+v, want %s", snap.Files, wantSize)
	}
	// 分片口径仍应自洽：2 片、位图全 1、pieceLength = 总长/片数
	if snap.NumPieces != "2" || snap.Bitfield != "c" {
		t.Fatalf("numPieces=%s bitfield=%s, want 2 / c", snap.NumPieces, snap.Bitfield)
	}
	if snap.PieceLength != strconv.FormatInt(info.Size()/2, 10) {
		t.Fatalf("pieceLength = %s, want %d", snap.PieceLength, info.Size()/2)
	}
}

// TestSnapshot_UnfinishedFallsBackToSegmentScale 未完成（如失败）时沿用分片量纲，
// 保证进度比仍可读且不越界。
func TestSnapshot_UnfinishedFallsBackToSegmentScale(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)
	tk := m.tasks[gids[0]]

	tk.setSegmentTotal(10)
	atomic.StoreInt64(&tk.completedLength, 4)

	snap := tk.Snapshot()
	const segScale = 1 << 20
	if snap.TotalLength != strconv.FormatInt(10*segScale, 10) {
		t.Fatalf("totalLength = %s, want %d", snap.TotalLength, 10*segScale)
	}
	if snap.CompletedLength != strconv.FormatInt(4*segScale, 10) {
		t.Fatalf("completedLength = %s, want %d", snap.CompletedLength, 4*segScale)
	}
	// 不能出现 completedLength > totalLength（按数值比较，避免字符串字典序误判）
	doneN, _ := strconv.ParseInt(snap.CompletedLength, 10, 64)
	totalN, _ := strconv.ParseInt(snap.TotalLength, 10, 64)
	if doneN > totalN {
		t.Fatalf("completedLength 超过 totalLength: %d > %d", doneN, totalN)
	}
}

// TestSnapshot_ConnectionsIsLiveConcurrency 未运行时 connections 为 0
// （而不是恒等于配置线程数）。
func TestSnapshot_ConnectionsIsLiveConcurrency(t *testing.T) {
	m := NewManager(1, 3, ".", "")
	gids := mustWaitingTasks(m, 1)
	tk := m.tasks[gids[0]]

	if got := tk.Snapshot().Connections; got != "0" {
		t.Fatalf("未运行任务 connections = %s, want 0", got)
	}
}
