package rpc

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"m3u8-downloader/task"
)

// wsDial 与 srv 的 /jsonrpc 建立 WebSocket 连接（手写握手，测试内不引第三方依赖）。
func wsDial(t *testing.T, ts *httptest.Server) (net.Conn, *bufio.Reader) {
	t.Helper()

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	keyBytes := make([]byte, 16)
	rand.Read(keyBytes)
	key := base64.StdEncoding.EncodeToString(keyBytes)

	fmt.Fprintf(conn, "GET /jsonrpc HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\n"+
		"Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		ts.Listener.Addr().String(), key)

	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read handshake: %v", err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("handshake status = %q, want 101", strings.TrimSpace(status))
	}
	// 读到空行结束响应头
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read handshake headers: %v", err)
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}
	return conn, br
}

// wsSendText 发送一个文本帧。
// 标准要求客户端帧掩码，但服务端 readWSFrame 对未掩码帧同样兼容，测试中直接复用写函数。
func wsSendText(t *testing.T, conn net.Conn, payload string) {
	t.Helper()
	if err := writeWSFrame(conn, []byte(payload), 1); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// wsReadFrame 在超时内读取一帧，超时返回 ok=false。
func wsReadFrame(t *testing.T, conn net.Conn, br *bufio.Reader, timeout time.Duration) (string, bool) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 1<<20)
	// bufio 已缓冲的字节需先消费
	if n := br.Buffered(); n > 0 {
		peeked, _ := br.Peek(n)
		if len(peeked) > 0 {
			// 直接交给底层 reader：把已缓冲内容读掉后按帧解析
			msg := make([]byte, n)
			if _, err := br.Read(msg); err == nil {
				return string(msg), true
			}
		}
	}
	msg, _, err := readWSFrame(conn, buf)
	if err != nil {
		return "", false
	}
	return string(msg), true
}

// TestWebSocket_NotifyOnStateChange 验证状态迁移会推送到已认证的 WebSocket 连接，
// 且帧格式为 aria2 通知（method + params[0].gid，无 id）。
func TestWebSocket_NotifyOnStateChange(t *testing.T) {
	mgr := task.NewManager(1, 3, t.TempDir(), "")
	srv := NewServer("127.0.0.1:0", mgr, "s3cret")
	ts := httptest.NewServer(http.HandlerFunc(srv.handleJSONRPC))
	defer ts.Close()

	conn, br := wsDial(t, ts)

	// 首条消息带 token：既完成认证，也验证响应仍走同一连接
	wsSendText(t, conn, `{"jsonrpc":"2.0","id":"1","method":"aria2.getVersion","params":["token:s3cret"]}`)
	reply, ok := wsReadFrame(t, conn, br, 3*time.Second)
	if !ok {
		t.Fatal("未收到 getVersion 响应")
	}
	if !strings.Contains(reply, "enabledFeatures") {
		t.Fatalf("getVersion 响应异常: %s", reply)
	}

	// 触发 waiting→active→error（连接被拒，失败很快）
	gid, err := mgr.AddURI("http://127.0.0.1:1/x.m3u8", task.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("addUri: %v", err)
	}

	got := map[string]bool{}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !(got["aria2.onDownloadStart"] && got["aria2.onDownloadError"]) {
		frame, ok := wsReadFrame(t, conn, br, 2*time.Second)
		if !ok {
			continue
		}
		t.Logf("frame: %s", frame)
		for _, m := range []string{"aria2.onDownloadStart", "aria2.onDownloadError"} {
			if strings.Contains(frame, `"method":"`+m+`"`) {
				got[m] = true
				if !strings.Contains(frame, gid) {
					t.Errorf("%s 帧未携带 gid %s: %s", m, gid, frame)
				}
				if strings.Contains(frame, `"id"`) {
					t.Errorf("通知帧不应含 id 字段: %s", frame)
				}
			}
		}
	}

	if !got["aria2.onDownloadStart"] {
		t.Fatal("未收到 aria2.onDownloadStart 通知")
	}
	if !got["aria2.onDownloadError"] {
		t.Fatal("未收到 aria2.onDownloadError 通知")
	}
}

// TestWebSocket_NotifyRequiresAuth 未认证连接不应收到通知。
func TestWebSocket_NotifyRequiresAuth(t *testing.T) {
	mgr := task.NewManager(1, 3, t.TempDir(), "")
	srv := NewServer("127.0.0.1:0", mgr, "s3cret")
	ts := httptest.NewServer(http.HandlerFunc(srv.handleJSONRPC))
	defer ts.Close()

	conn, br := wsDial(t, ts)
	// 不发带 token 的请求 → 未认证，不应注册到广播列表
	if _, err := mgr.AddURI("http://127.0.0.1:1/x.m3u8", task.Options{Dir: t.TempDir()}); err != nil {
		t.Fatalf("addUri: %v", err)
	}
	if frame, ok := wsReadFrame(t, conn, br, 1500*time.Millisecond); ok {
		t.Fatalf("未认证连接不应收到通知，却收到: %s", frame)
	}
}

// TestNotify_NoClients 无连接时广播应为空操作。
func TestNotify_NoClients(t *testing.T) {
	mgr := task.NewManager(1, 3, ".", "")
	srv := NewServer("127.0.0.1:0", mgr, "s3cret")
	srv.Notify("aria2.onDownloadComplete", "0123456789abcdef") // 不应 panic
}
