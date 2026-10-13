package rpc

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"m3u8-downloader/task"
)

// getJSONRPC 构造一个 aria2 GET 模式请求：token 放在 base64 params 数组首位。
func getJSONRPC(t *testing.T, secret, method, id string, params string) *httptest.ResponseRecorder {
	t.Helper()
	mgr := task.NewManager(1, 3, ".", "")
	srv := NewServer("127.0.0.1:0", mgr, secret)

	u := "/jsonrpc?method=" + url.QueryEscape(method) + "&id=" + url.QueryEscape(id)
	if params != "" {
		enc := base64.StdEncoding.EncodeToString([]byte(params))
		u += "&params=" + url.QueryEscape(enc)
	}
	req := httptest.NewRequest(http.MethodGet, u, nil)
	rr := httptest.NewRecorder()
	srv.handleGet(rr, req)
	return rr
}

// postJSONRPC 构造一个 aria2 POST 模式请求。
func postJSONRPC(t *testing.T, secret, body string) *httptest.ResponseRecorder {
	t.Helper()
	mgr := task.NewManager(1, 3, ".", "")
	srv := NewServer("127.0.0.1:0", mgr, secret)

	req := httptest.NewRequest(http.MethodPost, "/jsonrpc", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.handlePost(rr, req)
	return rr
}

// TestHandlePost_MulticallTokenInSubCalls AriaNg 的 retryTask 报文：
// method 为 system.multicall 时顶层不加 token（isSystemMethod），token 只存在于
// 各子调用的 params[0]。此前会因取不到顶层 token 而整体判定 Unauthorized。
func TestHandlePost_MulticallTokenInSubCalls(t *testing.T) {
	body := `{"jsonrpc":"2.0","method":"system.multicall","id":"1","params":[[` +
		`{"methodName":"aria2.tellStatus","params":["token:aword2020","ffffffffffffffff"]},` +
		`{"methodName":"aria2.getOption","params":["token:aword2020","ffffffffffffffff"]}]]}`

	rr := postJSONRPC(t, "aword2020", body)
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("want authorized multicall, got error %+v", resp.Error)
	}
	list, ok := resp.Result.([]interface{})
	if !ok || len(list) != 2 {
		t.Fatalf("result = %#v, want 2-element array", resp.Result)
	}
}

func TestHandlePost_MulticallWrongToken_Unauthorized(t *testing.T) {
	body := `{"jsonrpc":"2.0","method":"system.multicall","id":"1","params":[[` +
		`{"methodName":"aria2.getVersion","params":["token:wrong"]}]]}`

	rr := postJSONRPC(t, "aword2020", body)
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != 1 {
		t.Fatalf("want Unauthorized, got %+v", resp)
	}
}

func TestHandleGet_SecretNoToken_Unauthorized(t *testing.T) {
	rr := getJSONRPC(t, "aword2020", "aria2.getVersion", "abc", `["token:wrong"]`)
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != 1 {
		t.Fatalf("want Unauthorized, got %+v", resp)
	}
}

func TestHandleGet_TokenInParams_OK(t *testing.T) {
	// AriaNg GET 模式：params base64 = ["token:aword2020"]
	rr := getJSONRPC(t, "aword2020", "aria2.getVersion", "abc", `["token:aword2020"]`)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rr.Code)
	}
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if resp.ID != "abc" {
		t.Fatalf("id = %v, want abc", resp.ID)
	}
	if !strings.Contains(rr.Body.String(), "enabledFeatures") {
		t.Fatalf("unexpected body: %s", rr.Body.String())
	}
}

func TestHandleGet_NoTokenNoSecret_OK(t *testing.T) {
	// 服务端未启用鉴权，token 缺省也可调用
	rr := getJSONRPC(t, "", "aria2.getVersion", "abc", `[]`)
	var resp Response
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}
