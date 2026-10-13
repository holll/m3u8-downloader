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
		"aria2.changePosition":       handleChangePosition,
		"aria2.changeUri":            handleChangeUri,
		"aria2.getServers":           handleGetServers,
		"aria2.getPeers":             handleGetPeers,
		"aria2.forcePauseAll":        handlePauseAll,
		"aria2.forceShutdown":        handleShutdown,
		"aria2.saveSession":          handleSaveSession,
		"system.listNotifications":   handleListNotifications,
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

	// options 中数值型选项需同时接受数字与数字字符串：AriaNg 重试时直接回传
	// getOption 的结果，而 getOption 按 aria2 规范返回字符串。
	opts := task.Options{MaxWorkers: mgr.DefaultWorkers()}
	if len(arr) >= 2 {
		var m map[string]interface{}
		if err := json.Unmarshal(arr[1], &m); err == nil && m != nil {
			opts = task.OptionsFromMap(m, mgr.DefaultWorkers())
		}
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
	// enabledFeatures 必须如实声明：本程序不内置 BT/Metalink 引擎，
	// 告知 AriaNg 后其会自动隐藏种子上传等不支持的入口。
	return task.VersionInfo{
		Version: "1.37.0-m3u8-downloader",
		Features: []string{
			"Async DNS",
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

// multicallFault 构造 multicall 中单个子调用的失败项。
// 对应 aria2 RpcMethod::createErrorResponse：JSON-RPC 模式下为 {"code","message"} 对象，
// 且不像成功项那样再包一层数组。
func multicallFault(code int, message string) map[string]interface{} {
	return map[string]interface{}{"code": code, "message": message}
}

// handleMulticall 处理 system.multicall。
// aria2 报文规范（见 RpcMethodImpl.cc 的 SystemMulticallRpcMethod::execute，
// 其用 checkRequiredParam<List>(req, 0) 取调用列表）：
//   - params 为 [[{methodName,params}, ...]]，外层数组仅作包裹，调用列表是 params[0]；
//   - 成功项为 [value]（单元素数组），失败项为 {"code","message"} 对象。
//
// AriaNg 的重试功能依赖该格式（aria2TaskService.retryTask 取 response.data[i][0]）。
func handleMulticall(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var outer []json.RawMessage
	if err := json.Unmarshal(params, &outer); err != nil || len(outer) == 0 {
		return nil, errParams
	}

	var calls []multicallReq
	if err := json.Unmarshal(outer[0], &calls); err != nil {
		// 兼容未包外层的 [{methodName,params}, ...] 写法
		if err := json.Unmarshal(params, &calls); err != nil {
			return nil, errParams
		}
	}

	results := make([]interface{}, 0, len(calls))
	for _, call := range calls {
		h, ok := methodTable[call.MethodName]
		if !ok {
			results = append(results, multicallFault(1, "Method not found: "+call.MethodName))
			continue
		}
		// aria2 禁止递归 multicall
		if call.MethodName == "system.multicall" {
			results = append(results, multicallFault(1, "Recursive system.multicall forbidden."))
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
				results = append(results, multicallFault(rpcErr.Code, rpcErr.Message))
			} else {
				results = append(results, multicallFault(1, err.Error()))
			}
			continue
		}
		// 成功：包成单元素数组（aria2 规范）
		results = append(results, []interface{}{result})
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

// ============================== aria2.changePosition ==============================

// handleChangePosition 调整任务在等待队列中的位置。
// params: [gid, pos, how]，pos 为整数（可为负），how ∈ POS_SET/POS_CUR/POS_END。
func handleChangePosition(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(params, &arr); err != nil || len(arr) < 3 {
		return nil, errParams
	}
	var gid, how string
	var pos int
	if err := json.Unmarshal(arr[0], &gid); err != nil {
		return nil, errParams
	}
	if err := json.Unmarshal(arr[1], &pos); err != nil {
		return nil, errParams
	}
	if err := json.Unmarshal(arr[2], &how); err != nil {
		return nil, errParams
	}
	newPos, err := mgr.ChangePosition(gid, pos, how)
	if err != nil {
		return nil, &RPCError{Code: 1, Message: err.Error()}
	}
	return newPos, nil
}

// ============================== aria2.changeUri ==============================

// handleChangeUri 增减任务关联的 URI。
// params: [gid, fileIndex, delUris, addUris, position(可选)]。
func handleChangeUri(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(params, &arr); err != nil || len(arr) < 4 {
		return nil, errParams
	}
	var gid string
	var fileIndex int
	var delUris, addUris []string
	if err := json.Unmarshal(arr[0], &gid); err != nil {
		return nil, errParams
	}
	if err := json.Unmarshal(arr[1], &fileIndex); err != nil {
		return nil, errParams
	}
	if err := json.Unmarshal(arr[2], &delUris); err != nil {
		return nil, errParams
	}
	if err := json.Unmarshal(arr[3], &addUris); err != nil {
		return nil, errParams
	}
	del, add, err := mgr.ChangeUri(gid, fileIndex, delUris, addUris)
	if err != nil {
		return nil, &RPCError{Code: 1, Message: err.Error()}
	}
	return []int{del, add}, nil
}

// ============================== aria2.getServers ==============================

// handleGetServers 返回任务各文件的服务器连接信息（仅 active 状态）。
func handleGetServers(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	if err := parseParams(params, &gid); err != nil {
		return nil, err
	}
	servers, err := mgr.GetServers(gid)
	if err != nil {
		return nil, &RPCError{Code: 1, Message: err.Error()}
	}
	return servers, nil
}

// ============================== aria2.getPeers ==============================

// handleGetPeers 返回任务 peer 列表（m3u8 任务非 BT，任务存在即返回空数组）。
func handleGetPeers(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	var gid string
	if err := parseParams(params, &gid); err != nil {
		return nil, err
	}
	peers, err := mgr.GetPeers(gid)
	if err != nil {
		return nil, &RPCError{Code: 1, Message: err.Error()}
	}
	return peers, nil
}

// ============================== aria2.saveSession ==============================

// handleSaveSession 将会话立即写入 session-file。
func handleSaveSession(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	if err := mgr.SaveSessionNow(); err != nil {
		return nil, &RPCError{Code: 1, Message: err.Error()}
	}
	return "OK", nil
}

// ============================== system.listNotifications ==============================

func handleListNotifications(params json.RawMessage, mgr *task.Manager) (interface{}, error) {
	return []string{
		"aria2.onDownloadStart",
		"aria2.onDownloadPause",
		"aria2.onDownloadStop",
		"aria2.onDownloadComplete",
		"aria2.onDownloadError",
	}, nil
}
