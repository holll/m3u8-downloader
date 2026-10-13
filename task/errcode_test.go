package task

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"m3u8-downloader/dl"
)

func TestAria2ErrorCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, errCodeUnknown},
		{"404 资源不存在", &dl.HTTPStatusError{Status: 404, URL: "http://x/a.ts"}, errCodeResourceNotFound},
		{"410 资源不存在", &dl.HTTPStatusError{Status: 410}, errCodeResourceNotFound},
		{"401 鉴权失败", &dl.HTTPStatusError{Status: 401}, errCodeAuthFailed},
		{"403 鉴权失败", &dl.HTTPStatusError{Status: 403}, errCodeAuthFailed},
		{"407 代理鉴权失败", &dl.HTTPStatusError{Status: 407}, errCodeAuthFailed},
		{"500 响应异常", &dl.HTTPStatusError{Status: 500}, errCodeHTTPBadResponse},
		{"404 被包装后仍可识别", fmt.Errorf("fetch m3u8: %w", &dl.HTTPStatusError{Status: 404}), errCodeResourceNotFound},
		{"DNS 失败", &net.DNSError{Err: "no such host", Name: "bad.invalid"}, errCodeNameResolution},
		{"磁盘空间不足", syscall.ENOSPC, errCodeNotEnoughSpace},
		{"无权限", os.ErrPermission, errCodeFileOpenFailed},
		{"目录创建失败", &os.PathError{Op: "mkdir", Path: "/x", Err: os.ErrPermission}, errCodeDirCreateFailed},
		{"连接被拒", syscall.ECONNREFUSED, errCodeNetworkProblem},
		{"连接重置", syscall.ECONNRESET, errCodeNetworkProblem},
		{"超时", context.DeadlineExceeded, errCodeNetworkProblem},
		{"未知错误", fmt.Errorf("something odd"), errCodeUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := aria2ErrorCode(c.err); got != c.want {
				t.Fatalf("aria2ErrorCode(%v) = %s, want %s", c.err, got, c.want)
			}
		})
	}
}

// TestAria2ErrorCode_DNSError 确认真实的 DNS 解析失败（而非构造值）能归类为 19。
func TestAria2ErrorCode_DNSError(t *testing.T) {
	_, err := net.DialTimeout("tcp", "no-such-host.invalid:80", 2*time.Second)
	if err == nil {
		t.Skip("unexpected: resolution succeeded")
	}
	if got := aria2ErrorCode(err); got != errCodeNameResolution {
		t.Fatalf("aria2ErrorCode(%v) = %s, want 19", err, got)
	}
}

// TestTaskStart_SegmentFailure_ReportsRootCause RPC 路径的关键回归：
// 分片永久失败时任务必须失败并上报真实 errorCode，且不得把「仅拼接现存分片」
// 的残缺 mp4 当作成功产出。
func TestTaskStart_SegmentFailure_ReportsRootCause(t *testing.T) {
	playlist := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n" +
		"#EXT-X-MEDIA-SEQUENCE:0\n" +
		"#EXTINF:4.0,\nseg0.ts\n#EXTINF:4.0,\nseg1.ts\n#EXTINF:4.0,\nseg2.ts\n" +
		"#EXT-X-ENDLIST\n"

	mux := http.NewServeMux()
	mux.HandleFunc("/index.m3u8", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, playlist)
	})
	mux.HandleFunc("/seg0.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte{0x47, 0x40, 0x00, 0x10})
	})
	mux.HandleFunc("/seg1.ts", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	mux.HandleFunc("/seg2.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte{0x47, 0x40, 0x00, 0x10})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	outDir := t.TempDir()
	tk := NewTask(srv.URL+"/index.m3u8", Options{
		Dir: outDir, Out: "broken", MaxWorkers: 2, MaxRetry: 1,
	}, nil)

	done := make(chan struct{})
	tk.Start(func() { close(done) })
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("task did not finish")
	}

	if s := tk.Status(); s != StatusError {
		t.Fatalf("status = %s, want error（不得把缺少分片的视频报为完成）", s)
	}
	if tk.errorCode != errCodeResourceNotFound {
		t.Fatalf("errorCode = %s, want %s (404 → 资源不存在)", tk.errorCode, errCodeResourceNotFound)
	}
	if !strings.Contains(tk.errorMessage, "404") {
		t.Fatalf("errorMessage 未包含根因: %s", tk.errorMessage)
	}
	// 合并会产生残缺文件，因此任务失败时不应留下 mp4
	if _, err := os.Stat(filepath.Join(outDir, "broken.mp4")); err == nil {
		t.Fatal("任务已失败，不应产出残缺 mp4")
	}
}
