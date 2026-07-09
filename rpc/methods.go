package rpc

import (
	"encoding/json"

	"m3u8-downloader/task"
)

// ============================== 方法路由表 ==============================

// MethodHandler RPC 方法处理函数
type MethodHandler func(params json.RawMessage, mgr *task.Manager) (interface{}, error)

var methodTable map[string]MethodHandler

func init() {
	methodTable = map[string]MethodHandler{
		"aria2.addUri":               handleAddURI,
		"aria2.tellStatus":           handleTellStatus,
		"aria2.tellActive":           handleTellActive,
		"aria2.tellWaiting":          handleTellWaiting,
		"aria2.tellStopped":          handleTellStopped,
		"aria2.remove":               handleRemove,
		"aria2.removeDownloadResult": handleRemoveDownloadResult,
		"aria2.pause":                handlePause,
		"aria2.unpause":              handleUnpause,
		"aria2.getGlobalStat":        handleGetGlobalStat,
		"aria2.getVersion":           handleGetVersion,
		"aria2.getSessionInfo":       handleGetSessionInfo,
		"system.multicall":           handleMulticall,
		"system.listMethods":         handleListMethods,
		"aria2.getGlobalOption":      handleGetGlobalOption,
		"aria2.changeGlobalOption":   handleChangeGlobalOption,
		"aria2.shutdown":             handleShutdown,
		"aria2.forceRemove":          handleRemove,
		"aria2.forcePause":           handlePause,
		"aria2.pauseAll":             handlePauseAll,
		"aria2.unpauseAll":           handleUnpauseAll,
		"aria2.purgeDownloadResult":  handlePurgeDownloadResult,
		"aria2.getOption":            handleGetOption,
		"aria2.changeOption":         handleChangeOption,
		"aria2.getUris":              handleGetUris,
		"aria2.getFiles":             handleGetFiles,
	}
}

// Dispatch 路由方法名到处理器
func Dispatch(method string) (MethodHandler, bool) {
	h, ok := methodTable[method]
	return h, ok
}

// ============================== aria2.addUri ==============================

func handleAddURI(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	// params: [ [url1, url2...], options ]
	var arr []json.RawMessage
	if err := json.Unmarshal(params, &arr); err != nil || len(arr) < 1 {
		return nil, errParams
	}

	var urls []string
	if err := json.Unmarshal(arr[0], &urls); err != nil || len(urls) == 0 {
		return nil, errParams
	}

	opts := task.Options{MaxWorkers: mgr.DefaultWorkers()}
	if len(arr) >= 2 {
		json.Unmarshal(arr[1], &opts)
	}

	// 仅使用第一个 URL
	gid, err := mgr.AddURI(urls[0], opts)
	if err != nil {
		return nil, &RPCError{Code: ErrInternal, Message: err.Error()}
	}
	return gid, nil
}

// ============================== aria2.tellStatus ==============================

func handleTellStatus(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	if err := parseParams(params, &gid); err != nil {
		return nil, err
	}
	return mgr.Status(gid)
}

// ============================== aria2.tellActive ==============================

func handleTellActive(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return mgr.TellActive(), nil
}

// ============================== aria2.tellWaiting ==============================

func handleTellWaiting(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(params, &arr); err != nil {
		return mgr.TellWaiting(0, 1000), nil
	}
	offset, num := 0, 1000
	if len(arr) >= 1 {
		json.Unmarshal(arr[0], &offset)
	}
	if len(arr) >= 2 {
		json.Unmarshal(arr[1], &num)
	}
	return mgr.TellWaiting(offset, num), nil
}

// ============================== aria2.tellStopped ==============================

func handleTellStopped(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(params, &arr); err != nil {
		return mgr.TellStopped(0, 1000), nil
	}
	offset, num := 0, 1000
	if len(arr) >= 1 {
		json.Unmarshal(arr[0], &offset)
	}
	if len(arr) >= 2 {
		json.Unmarshal(arr[1], &num)
	}
	return mgr.TellStopped(offset, num), nil
}

// ============================== aria2.remove ==============================

func handleRemove(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	if err := parseParams(params, &gid); err != nil {
		return nil, err
	}
	if err := mgr.Remove(gid); err != nil {
		return nil, &RPCError{Code: 1, Message: err.Error()}
	}
	return gid, nil
}

// ============================== aria2.removeDownloadResult ==============================

func handleRemoveDownloadResult(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	if err := parseParams(params, &gid); err != nil {
		return nil, err
	}
	if err := mgr.RemoveDownloadResult(gid); err != nil {
		return nil, &RPCError{Code: 1, Message: err.Error()}
	}
	return "OK", nil
}

// ============================== aria2.pause ==============================

func handlePause(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	if err := parseParams(params, &gid); err != nil {
		return nil, err
	}
	if err := mgr.Pause(gid); err != nil {
		return nil, &RPCError{Code: 1, Message: err.Error()}
	}
	return gid, nil
}

// ============================== aria2.unpause ==============================

func handleUnpause(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	if err := parseParams(params, &gid); err != nil {
		return nil, err
	}
	if err := mgr.Unpause(gid); err != nil {
		return nil, &RPCError{Code: 1, Message: err.Error()}
	}
	return gid, nil
}

// ============================== aria2.getGlobalStat ==============================

func handleGetGlobalStat(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return mgr.GlobalStat(), nil
}

// ============================== aria2.getVersion ==============================

func handleGetVersion(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return task.VersionInfo{
		Version: "m3u8-downloader/1.0",
		Features: []string{
			"Async DNS",
			"BitTorrent",
		},
	}, nil
}

// ============================== aria2.getSessionInfo ==============================

func handleGetSessionInfo(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return task.SessionInfo{ID: "m3u8-downloader-session"}, nil
}

// ============================== system.multicall ==============================

// multicallReq 解析 aria2 system.multicall 的子调用（methodName + params 数组）。
type multicallReq struct {
	MethodName string            `json:"methodName"`
	Params     []json.RawMessage `json:"params"`
}

func handleMulticall(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	// params 格式: [ [{methodName:"...", params:[...]}, ...] ]
	// 先提取内层数组
	var wrapper []multicallReq
	if err := json.Unmarshal(params, &wrapper); err != nil {
		return nil, errParams
	}

	results := make([]interface{}, 0, len(wrapper))
	for _, call := range wrapper {
		h, ok := methodTable[call.MethodName]
		if !ok {
			// aria2 规范：multicall 错误返回 [faultCode, faultString]
			results = append(results, []interface{}{1, "Method not found: " + call.MethodName})
			continue
		}
		// 将 params 数组重新序列化为 json.RawMessage
		paramsBytes, _ := json.Marshal(call.Params)
		paramsRaw := json.RawMessage(paramsBytes)
		// 移除 token（aria2 兼容）
		paramsRaw = stripToken(paramsRaw)
		result, err := h(paramsRaw, mgr)
		if err != nil {
			if rpcErr, ok := err.(*RPCError); ok {
				results = append(results, []interface{}{rpcErr.Code, rpcErr.Message})
			} else {
				results = append(results, []interface{}{1, err.Error()})
			}
			continue
		}
		// 成功：直接返回结果值（aria2 规范）
		results = append(results, result)
	}
	return results, nil
}

// ============================== 补充方法 ==============================

// --- system.listMethods ---

func handleListMethods(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	methods := make([]string, 0, len(methodTable))
	for m := range methodTable {
		methods = append(methods, m)
	}
	return methods, nil
}

// --- 全局选项 ---

func handleGetGlobalOption(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return mgr.GlobalOption(), nil
}

func handleChangeGlobalOption(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var opts map[string]interface{}
	if err := parseParams(params, &opts); err != nil {
		return nil, err
	}
	if err := mgr.ChangeGlobalOption(opts); err != nil {
		return nil, &RPCError{Code: ErrInternal, Message: err.Error()}
	}
	return "OK", nil
}

// --- 关闭 ---

func handleShutdown(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	mgr.RequestShutdown()
	return "OK", nil
}

// --- 批量清理 ---

func handlePurgeDownloadResult(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	mgr.PurgeDownloadResult()
	return "OK", nil
}

// --- 单任务选项 ---

func handleGetOption(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	if err := parseParams(params, &gid); err != nil {
		return nil, err
	}
	return mgr.GetOption(gid)
}

func handleChangeOption(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	var opts map[string]interface{}
	if err := parseParams2(params, &gid, &opts); err != nil {
		return nil, err
	}
	if err := mgr.ChangeOption(gid, opts); err != nil {
		return nil, &RPCError{Code: 1, Message: err.Error()}
	}
	return "OK", nil
}

// --- URI / Files ---

func handleGetUris(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	if err := parseParams(params, &gid); err != nil {
		return nil, err
	}
	return mgr.GetUris(gid)
}

func handleGetFiles(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	if err := parseParams(params, &gid); err != nil {
		return nil, err
	}
	return mgr.GetFiles(gid)
}

// --- 全部暂停/恢复 ---

func handlePauseAll(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	mgr.PauseAll()
	return "OK", nil
}

func handleUnpauseAll(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	mgr.UnpauseAll()
	return "OK", nil
}
