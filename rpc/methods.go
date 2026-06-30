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
		"aria2.getGlobalOption":      handleGetGlobalOption,
		"aria2.changeGlobalOption":   handleChangeGlobalOption,
		"aria2.shutdown":             handleShutdown,
		"aria2.forceRemove":          handleRemove,
		"aria2.forcePause":           handlePause,
		"aria2.pauseAll":             handlePauseAll,
		"aria2.unpauseAll":           handleUnpauseAll,
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

func handleMulticall(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var calls []Request
	if err := json.Unmarshal(params, &calls); err != nil {
		return nil, errParams
	}

	results := make([]Response, 0, len(calls))
	for _, req := range calls {
		h, ok := methodTable[req.Method]
		if !ok {
			results = append(results, newError(parseID(req.ID), errMethod))
			continue
		}
		result, err := h(req.Params, mgr)
		if err != nil {
			if rpcErr, ok := err.(*RPCError); ok {
				results = append(results, newError(parseID(req.ID), rpcErr))
			} else {
				results = append(results, newError(parseID(req.ID), errInternal))
			}
			continue
		}
		results = append(results, newResponse(parseID(req.ID), result))
	}
	return results, nil
}

// ============================== 补充方法 (stub) ==============================

func handleGetGlobalOption(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return map[string]string{
		"max-concurrent-downloads":   "1",
		"max-connection-per-server":  "3",
		"max-overall-download-limit": "0",
		"dir":                        ".",
	}, nil
}

func handleChangeGlobalOption(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return "OK", nil
}

func handleShutdown(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return "OK", nil
}

func handlePauseAll(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return "OK", nil
}

func handleUnpauseAll(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return "OK", nil
}
