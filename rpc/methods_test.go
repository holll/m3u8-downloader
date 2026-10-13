package rpc

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"m3u8-downloader/task"
)

// callMethod 直接调用路由表中的 handler（params 用 JSON 数组）。
func callMethod(t *testing.T, method string, params []interface{}, mgr *task.Manager) Response {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	h, ok := Dispatch(method)
	if !ok {
		t.Fatalf("method not registered: %s", method)
	}
	result, err := h(raw, mgr)
	if err != nil {
		if rpcErr, ok := err.(*RPCError); ok {
			return newError("test", rpcErr)
		}
		return newError("test", &RPCError{Code: ErrInternal, Message: err.Error()})
	}
	return newResponse("test", result)
}

func TestNewMethodsRegistered(t *testing.T) {
	for _, m := range []string{
		"aria2.changePosition",
		"aria2.changeUri",
		"aria2.getServers",
		"aria2.getPeers",
		"aria2.forcePauseAll",
		"aria2.forceShutdown",
		"aria2.saveSession",
		"system.listNotifications",
	} {
		if _, ok := Dispatch(m); !ok {
			t.Fatalf("method %s not registered", m)
		}
	}
}

func TestHandleSaveSession(t *testing.T) {
	// 未配置 session-file → code 1
	mgr := task.NewManager(1, 3, ".", "")
	resp := callMethod(t, "aria2.saveSession", nil, mgr)
	if resp.Error == nil {
		t.Fatal("want error when session-file not configured")
	}
	if resp.Error.Code != 1 {
		t.Fatalf("error code = %d, want 1", resp.Error.Code)
	}

	// 已配置 → "OK"
	path := filepath.Join(t.TempDir(), "m.session")
	mgr2 := task.NewManager(1, 3, ".", path)
	resp = callMethod(t, "aria2.saveSession", nil, mgr2)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if resp.Result != "OK" {
		t.Fatalf("result = %v, want OK", resp.Result)
	}
}

func TestHandleListNotifications(t *testing.T) {
	mgr := task.NewManager(1, 3, ".", "")
	resp := callMethod(t, "system.listNotifications", nil, mgr)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	list, ok := resp.Result.([]string)
	if !ok {
		t.Fatalf("result type = %T, want []string", resp.Result)
	}
	want := []string{"aria2.onDownloadStart", "aria2.onDownloadPause", "aria2.onDownloadStop", "aria2.onDownloadComplete", "aria2.onDownloadError"}
	for _, w := range want {
		found := false
		for _, got := range list {
			if got == w {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("notification %s missing in %v", w, list)
		}
	}
}

func TestHandleGetVersion_NoBtClaim(t *testing.T) {
	mgr := task.NewManager(1, 3, ".", "")
	resp := callMethod(t, "aria2.getVersion", nil, mgr)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	vi, ok := resp.Result.(task.VersionInfo)
	if !ok {
		t.Fatalf("result type = %T, want task.VersionInfo", resp.Result)
	}
	for _, f := range vi.Features {
		if f == "BitTorrent" || f == "Metalink" {
			t.Fatalf("enabledFeatures must not claim %s", f)
		}
	}
	if vi.Version == "" {
		t.Fatal("version is empty")
	}
}

func TestHandleChangeUri_MissingTask(t *testing.T) {
	mgr := task.NewManager(1, 3, ".", "")
	resp := callMethod(t, "aria2.changeUri", []interface{}{
		"ffffffffffffffff", 1, []string{}, []string{"http://x.m3u8"},
	}, mgr)
	if resp.Error == nil {
		t.Fatal("want error for missing gid")
	}
}

// TestHandleMulticall_Aria2WireFormat 复刻 AriaNg 的重试流程：aria2 的
// system.multicall 要求 params 为 [[{methodName,params},...]]（外层再包一层数组），
// 且每个成功子调用必须在结果中包成单元素数组 [value]，失败则为 {"code","message"} 对象。
// 参考 aria2 源码 RpcMethodImpl.cc 的 SystemMulticallRpcMethod::execute。
func TestHandleMulticall_Aria2WireFormat(t *testing.T) {
	mgr := task.NewManager(1, 3, ".", "")

	params := []interface{}{
		[]interface{}{
			map[string]interface{}{"methodName": "aria2.getVersion", "params": []interface{}{}},
			map[string]interface{}{"methodName": "aria2.getSessionInfo", "params": []interface{}{}},
			map[string]interface{}{"methodName": "aria2.noSuchMethod", "params": []interface{}{}},
		},
	}

	resp := callMethod(t, "system.multicall", params, mgr)
	if resp.Error != nil {
		t.Fatalf("multicall returned error: %+v", resp.Error)
	}

	list, ok := resp.Result.([]interface{})
	if !ok {
		t.Fatalf("result type = %T, want []interface{}", resp.Result)
	}
	if len(list) != 3 {
		t.Fatalf("len(result) = %d, want 3", len(list))
	}

	// 成功项必须包成单元素数组，AriaNg 取 data[i][0]
	wrapped, ok := list[0].([]interface{})
	if !ok || len(wrapped) != 1 {
		t.Fatalf("result[0] = %#v, want single-element array", list[0])
	}
	if _, ok := wrapped[0].(task.VersionInfo); !ok {
		t.Fatalf("result[0][0] type = %T, want task.VersionInfo", wrapped[0])
	}

	// 失败项为 {"code","message"} 对象，不包数组
	fault, ok := list[2].(map[string]interface{})
	if !ok {
		t.Fatalf("result[2] = %#v, want fault object", list[2])
	}
	if _, ok := fault["message"]; !ok {
		t.Fatalf("fault object missing message: %#v", fault)
	}
}

// TestHandleMulticall_WithToken 校验 AriaNg 在配置 rpc-secret 时的实际报文：
// 顶层不加 token，token 位于每个子调用的 params[0]。
func TestHandleMulticall_WithToken(t *testing.T) {
	mgr := task.NewManager(1, 3, ".", "")

	params := []interface{}{
		[]interface{}{
			map[string]interface{}{
				"methodName": "aria2.getVersion",
				"params":     []interface{}{"token:aword2020"},
			},
		},
	}

	resp := callMethod(t, "system.multicall", params, mgr)
	if resp.Error != nil {
		t.Fatalf("multicall returned error: %+v", resp.Error)
	}
	list, ok := resp.Result.([]interface{})
	if !ok || len(list) != 1 {
		t.Fatalf("result = %#v, want 1-element array", resp.Result)
	}
	wrapped, ok := list[0].([]interface{})
	if !ok || len(wrapped) != 1 {
		t.Fatalf("result[0] = %#v, want single-element array (token stripped)", list[0])
	}
	if _, ok := wrapped[0].(task.VersionInfo); !ok {
		t.Fatalf("result[0][0] type = %T, want task.VersionInfo (token must be stripped)", wrapped[0])
	}
}

func TestHandleGetPeers_MissingTask(t *testing.T) {
	mgr := task.NewManager(1, 3, ".", "")
	resp := callMethod(t, "aria2.getPeers", []interface{}{"ffffffffffffffff"}, mgr)
	if resp.Error == nil {
		t.Fatal("want error for missing gid")
	}
	if resp.Error.Code != 1 {
		t.Fatalf("error code = %d, want 1", resp.Error.Code)
	}
}
