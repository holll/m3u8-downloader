package dl

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testPlaylist = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n" +
	"#EXT-X-MEDIA-SEQUENCE:0\n" +
	"#EXTINF:4.0,\nseg0.ts\n#EXTINF:4.0,\nseg1.ts\n#EXTINF:4.0,\nseg2.ts\n" +
	"#EXT-X-ENDLIST\n"

// newSegmentServer 起一个测试 m3u8 源；failingSeg 指定返回 404 的分片名（空则全部成功）。
func newSegmentServer(t *testing.T, failingSeg string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/index.m3u8", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, testPlaylist)
	})
	segHandler := func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimPrefix(r.URL.Path, "/") == failingSeg {
			http.NotFound(w, r)
			return
		}
		writeTS(w, r)
	}
	for _, name := range []string{"/seg0.ts", "/seg1.ts", "/seg2.ts"} {
		mux.HandleFunc(name, segHandler)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func writeTS(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte{0x47, 0x40, 0x00, 0x10})
}

// TestSegmentError_KeepsRootCause 分片永久失败时，必须保留根因（HTTP 404），
// 否则上层只能拿到 Merge 的「下载似乎未完成」，无法判断真实失败原因。
func TestSegmentError_KeepsRootCause(t *testing.T) {
	srv := newSegmentServer(t, "seg1.ts")

	d := New(Config{
		M3U8URL: srv.URL + "/index.m3u8", OutputDir: t.TempDir(),
		MaxWorkers: 2, MaxRetry: 1, HostType: "v1", Quiet: true,
	})
	if err := d.Parse(); err != nil {
		t.Fatalf("parse: %v", err)
	}
	d.DownloadAll()

	err := d.SegmentError()
	if err == nil {
		t.Fatal("want segment error for the 404 segment, got nil")
	}

	var httpErr *HTTPStatusError
	if !errors.As(err, &httpErr) {
		t.Fatalf("err = %v (%T), want *HTTPStatusError extractable", err, err)
	}
	if httpErr.Status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", httpErr.Status)
	}
	if httpErr.URL == "" {
		t.Fatal("HTTPStatusError.URL 应记录出错的请求地址")
	}
	// 重试次数信息不应覆盖根因（%w 链可同时读出两者）
	if got := fmt.Sprint(err); got == "" {
		t.Fatal("empty error text")
	}
	t.Logf("segment error = %v", err)
}

// TestSegmentError_NilWhenAllSucceed 全部分片成功时不得误报错误。
func TestSegmentError_NilWhenAllSucceed(t *testing.T) {
	srv := newSegmentServer(t, "")

	d := New(Config{
		M3U8URL: srv.URL + "/index.m3u8", OutputDir: t.TempDir(),
		MaxWorkers: 2, MaxRetry: 1, HostType: "v1", Quiet: true,
	})
	if err := d.Parse(); err != nil {
		t.Fatalf("parse: %v", err)
	}
	d.DownloadAll()

	if err := d.SegmentError(); err != nil {
		t.Fatalf("SegmentError = %v, want nil", err)
	}
}
