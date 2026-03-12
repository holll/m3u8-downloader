package app

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/websocket"
)

// runAPIServer 启动 aria2 风格 JSON-RPC 服务。
// 支持 HTTP POST + WebSocket，两条路径 /jsonrpc 和 /rpc 共用一套处理。
func runAPIServer(addr string, maxJobs int, rpcSecret string) {
	if maxJobs <= 0 {
		maxJobs = 1
	}
	manager := &DownloadManager{
		limiter:   make(chan struct{}, maxJobs),
		tasks:     map[string]*TaskStatus{},
		rpcSecret: rpcSecret,
	}
	http.HandleFunc("/jsonrpc", manager.handleJSONRPC)
	http.HandleFunc("/rpc", manager.handleJSONRPC)
	fmt.Printf("[API] json-rpc listening on %s, max parallel jobs: %d\n", addr, maxJobs)
	checkErr(http.ListenAndServe(addr, nil))
}

func (m *DownloadManager) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	if isWebSocketRequest(r) {
		websocket.Handler(m.handleJSONRPCWebSocket).ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(marshalRPCResponse(map[string]interface{}{"jsonrpc": "2.0", "id": nil, "error": map[string]interface{}{"code": -32700, "message": "invalid body"}}))
		return
	}
	debugf("http rpc request: path=%s remote=%s", r.URL.Path, r.RemoteAddr)
	resp := m.processRPCBody(body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(resp)
}

func (m *DownloadManager) handleJSONRPCWebSocket(conn *websocket.Conn) {
	defer conn.Close()
	for {
		var message []byte
		if err := websocket.Message.Receive(conn, &message); err != nil {
			return
		}
		resp := m.processRPCBody(message)
		if err := websocket.Message.Send(conn, string(resp)); err != nil {
			return
		}
	}
}

func isWebSocketRequest(r *http.Request) bool {
	upgrade := strings.ToLower(r.Header.Get("Upgrade"))
	connection := strings.ToLower(r.Header.Get("Connection"))
	return upgrade == "websocket" && strings.Contains(connection, "upgrade")
}

// processRPCBody 执行统一的 JSON-RPC 流程：解析 -> 鉴权 -> 方法分发 -> 响应封装。
func (m *DownloadManager) processRPCBody(body []byte) []byte {
	var req struct {
		JSONRPC string            `json:"jsonrpc"`
		ID      interface{}       `json:"id"`
		Method  string            `json:"method"`
		Params  []json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return marshalRPCResponse(map[string]interface{}{"jsonrpc": "2.0", "id": nil, "error": map[string]interface{}{"code": -32700, "message": "parse error"}})
	}
	params, rpcErr := m.authorize(req.Method, req.Params)
	if rpcErr != nil {
		return marshalRPCResponse(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "error": map[string]interface{}{"code": rpcErr.Code, "message": rpcErr.Message}})
	}
	debugf("rpc dispatch: method=%s params=%d", req.Method, len(params))
	result, rpcErr := m.dispatch(req.Method, params)
	if rpcErr != nil {
		return marshalRPCResponse(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "error": map[string]interface{}{"code": rpcErr.Code, "message": rpcErr.Message}})
	}
	return marshalRPCResponse(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result})
}

func (m *DownloadManager) authorize(method string, params []json.RawMessage) ([]json.RawMessage, *RPCError) {
	if m.rpcSecret == "" {
		return params, nil
	}
	if len(params) == 0 {
		debugf("rpc auth failed: method=%s", method)
		return nil, &RPCError{Code: 1, Message: "Unauthorized"}
	}
	var token string
	if err := json.Unmarshal(params[0], &token); err == nil && token == "token:"+m.rpcSecret {
		debugf("rpc auth passed: method=%s", method)
		return params[1:], nil
	}
	return nil, &RPCError{Code: 1, Message: "Unauthorized"}
}

func marshalRPCResponse(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		fallback := []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"internal error"}}`)
		return fallback
	}
	return b
}

type RPCError struct {
	Code    int
	Message string
}

func (m *DownloadManager) dispatch(method string, params []json.RawMessage) (interface{}, *RPCError) {
	switch method {
	case "aria2.addUri":
		return m.rpcAddURI(params)
	case "aria2.tellStatus":
		return m.rpcTellStatus(params)
	case "aria2.tellActive":
		return m.rpcTellActive(), nil
	case "aria2.tellWaiting":
		return m.rpcTellByStatus(params, "waiting")
	case "aria2.tellStopped":
		return m.rpcTellByStatus(params, "stopped")
	case "aria2.getGlobalStat":
		return m.rpcGlobalStat(), nil
	case "aria2.getGlobalOption":
		return map[string]string{"max-concurrent-downloads": strconv.Itoa(cap(m.limiter))}, nil
	case "aria2.getVersion":
		return map[string]interface{}{"version": "1.0.0", "enabledFeatures": []string{"JSON-RPC", "m3u8"}}, nil
	case "system.multicall":
		return m.rpcMultiCall(params)
	default:
		return nil, &RPCError{Code: -32601, Message: "method not found"}
	}
}

func (m *DownloadManager) rpcAddURI(params []json.RawMessage) (interface{}, *RPCError) {
	if len(params) < 1 {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	var uris []string
	if err := json.Unmarshal(params[0], &uris); err != nil || len(uris) == 0 {
		return nil, &RPCError{Code: -32602, Message: "invalid uri list"}
	}
	job := DownloadJob{M3U8URL: uris[0], MaxGoroutines: *nFlag, HostType: *htFlag, MovieName: *oFlag, AutoClear: *rFlag, Cookie: *cFlag, Insecure: *sFlag, SavePath: *spFlag}
	if len(params) > 1 {
		var opts map[string]interface{}
		if err := json.Unmarshal(params[1], &opts); err == nil {
			applyJobOptions(&job, opts)
		}
	}
	gid := newGID()
	m.mu.Lock()
	m.tasks[gid] = &TaskStatus{GID: gid, Status: "waiting", Result: expectedOutputPath(job), Error: ""}
	m.mu.Unlock()
	go m.runTask(gid, job)
	return gid, nil
}

func (m *DownloadManager) rpcTellStatus(params []json.RawMessage) (interface{}, *RPCError) {
	if len(params) < 1 {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	var gid string
	if err := json.Unmarshal(params[0], &gid); err != nil || gid == "" {
		return nil, &RPCError{Code: -32602, Message: "invalid gid"}
	}
	m.mu.RLock()
	task, ok := m.tasks[gid]
	m.mu.RUnlock()
	if !ok {
		return nil, &RPCError{Code: 1, Message: "gid not found"}
	}
	return toAria2Status(task), nil
}

func (m *DownloadManager) rpcTellActive() []map[string]interface{} {
	return m.collectByStatus("active")
}

func (m *DownloadManager) rpcTellByStatus(params []json.RawMessage, target string) (interface{}, *RPCError) {
	if len(params) < 2 {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	return m.collectByStatus(target), nil
}

func (m *DownloadManager) collectByStatus(target string) []map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]map[string]interface{}, 0)
	for _, t := range m.tasks {
		if target == "stopped" {
			if t.Status == "complete" || t.Status == "error" {
				res = append(res, toAria2Status(t))
			}
			continue
		}
		if t.Status == target {
			res = append(res, toAria2Status(t))
		}
	}
	return res
}

func (m *DownloadManager) rpcGlobalStat() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var active, waiting, stopped int
	for _, t := range m.tasks {
		switch t.Status {
		case "active":
			active++
		case "waiting":
			waiting++
		case "complete", "error":
			stopped++
		}
	}
	return map[string]string{
		"downloadSpeed": "0",
		"uploadSpeed":   "0",
		"numActive":     strconv.Itoa(active),
		"numWaiting":    strconv.Itoa(waiting),
		"numStopped":    strconv.Itoa(stopped),
	}
}

func (m *DownloadManager) rpcMultiCall(params []json.RawMessage) (interface{}, *RPCError) {
	if len(params) < 1 {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	var calls []struct {
		MethodName string            `json:"methodName"`
		Params     []json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(params[0], &calls); err != nil {
		return nil, &RPCError{Code: -32602, Message: "invalid multicall params"}
	}
	results := make([][]interface{}, 0, len(calls))
	for _, c := range calls {
		ret, rpcErr := m.dispatch(c.MethodName, c.Params)
		if rpcErr != nil {
			results = append(results, []interface{}{map[string]interface{}{"code": rpcErr.Code, "message": rpcErr.Message}})
			continue
		}
		results = append(results, []interface{}{ret})
	}
	return results, nil
}

func (m *DownloadManager) runTask(gid string, job DownloadJob) {
	m.limiter <- struct{}{}
	m.updateTask(gid, "active", "", "")
	debugf("task active: gid=%s url=%s", gid, job.M3U8URL)
	result, err := runDownload(job, func(done, total int) {
		m.updateTaskProgress(gid, done, total)
	})
	if err != nil {
		m.updateTask(gid, "error", "", err.Error())
		<-m.limiter
		return
	}
	m.updateTask(gid, "complete", result, "")
	debugf("task complete: gid=%s result=%s", gid, result)
	<-m.limiter
}

func (m *DownloadManager) updateTask(gid, status, result, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[gid]; ok {
		task.Status = status
		task.Result = result
		task.Error = errMsg
	}
}

func (m *DownloadManager) updateTaskProgress(gid string, done, total int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[gid]; ok {
		if total >= 0 {
			task.TotalSegments = total
		}
		if done >= 0 {
			task.CompletedSegments = done
		}
	}
}

func toAria2Status(task *TaskStatus) map[string]interface{} {
	status := task.Status
	if status == "complete" || status == "error" {
		status = "complete"
		if task.Error != "" {
			status = "error"
		}
	}
	filePath := task.Result
	if filePath == "" {
		filePath = task.GID + ".mp4"
	}
	errorCode := ""
	if task.Error != "" {
		errorCode = "1"
	}
	return map[string]interface{}{
		"gid":             task.GID,
		"status":          status,
		"totalLength":     strconv.Itoa(task.TotalSegments),
		"completedLength": strconv.Itoa(task.CompletedSegments),
		"downloadSpeed":   "0",
		"errorCode":       errorCode,
		"errorMessage":    task.Error,
		"files": []map[string]interface{}{
			{"index": "1", "path": filePath, "uris": []map[string]string{}},
		},
	}
}

func applyJobOptions(job *DownloadJob, options map[string]interface{}) {
	if out, ok := options["out"].(string); ok && out != "" {
		job.OutputName = out
		job.MovieName = strings.TrimSuffix(out, filepath.Ext(out))
	}
	if dir, ok := options["dir"].(string); ok {
		job.SavePath = dir
	}
	if header, ok := options["header"].(string); ok {
		job.Cookie = header
	}
	if split, ok := options["split"].(float64); ok && int(split) > 0 {
		job.MaxGoroutines = int(split)
	}
	if split, ok := options["split"].(string); ok {
		if n, err := strconv.Atoi(split); err == nil && n > 0 {
			job.MaxGoroutines = n
		}
	}
}

func expectedOutputPath(job DownloadJob) string {
	pwd, _ := os.Getwd()
	if job.SavePath != "" {
		pwd = job.SavePath
	}
	out := job.OutputName
	if out == "" {
		name := job.MovieName
		if name == "" {
			name = "movie"
		}
		out = name + ".mp4"
	}
	return filepath.Join(pwd, out)
}

func newGID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano()+rand.Int63())
}
