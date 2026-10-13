package rpc

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"m3u8-downloader/task"
)

// ============================== Server ==============================

// Server JSON-RPC 2.0 HTTP 服务
type Server struct {
	manager  *task.Manager
	secret   string
	addr     string
	hs       *http.Server
	ln       net.Listener // 手动创建的 listener，用于显式关闭
	reqCount int64        // atomic request counter

	wsMu      sync.RWMutex           // 保护 wsClients
	wsClients map[*wsClient]struct{} // 已认证的 WebSocket 连接（用于推送通知）
}

// NewServer 创建服务实例，并把自身的通知广播注册为 Manager 的状态迁移回调。
func NewServer(addr string, mgr *task.Manager, secret string) *Server {
	s := &Server{
		manager:   mgr,
		secret:    secret,
		addr:      addr,
		wsClients: make(map[*wsClient]struct{}),
	}
	mgr.SetEventCallback(s.Notify)
	return s
}

// Start 启动 HTTP 服务，阻塞直到 Stop() 或监听失败。
// 使用 SO_REUSEADDR 允许快速重启（Windows 上进程退出后端口可立即复用）。
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/jsonrpc", s.handleJSONRPC)

	ln, err := createListener(s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	s.ln = ln // 保存以便显式关闭

	s.hs = &http.Server{
		Handler: withCORS(mux),
	}
	fmt.Printf("[RPC] listening on http://%s/jsonrpc\n", s.addr)
	return s.hs.Serve(ln)
}

// Stop 立即关闭（主动关闭 listener + http.Server）
func (s *Server) Stop() error {
	// 先关闭 listener，确保 Accept 立即返回
	if s.ln != nil {
		s.ln.Close()
	}
	if s.hs != nil {
		return s.hs.Close()
	}
	return nil
}

// ============================== HTTP Handler ==============================

func (s *Server) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	_ = atomic.AddInt64(&s.reqCount, 1)

	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("[RPC] panic: %v", rec)
			writeError(w, nil, errInternal)
		}
	}()

	// WebSocket 升级检测
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		// WebSocket token 在首条消息的 params[0] 中校验（由 stripToken 处理）
		s.handleWebSocket(w, r)
		return
	}

	switch r.Method {
	case http.MethodPost:
		s.handlePost(w, r)
	case http.MethodGet:
		s.handleGet(w, r)
	default:
		writeError(w, nil, errInvalidReq)
	}
}

func (s *Server) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, nil, errParse)
		return
	}

	// 鉴权：支持 URL query / header / params[0] 三种方式
	if s.secret != "" {
		if !s.authenticate(r) && !s.authenticateBody(body) {
			writeError(w, nil, &RPCError{Code: 1, Message: "Unauthorized"})
			return
		}
	}

	if len(body) > 0 && body[0] == '[' {
		s.handleBatch(w, body)
		return
	}

	s.handleSingle(w, body)
}

// handleGet 处理 GET 请求（aria2 兼容：?method=M&id=I&params=BASE64_JSON）
// AriaNg 的 GET 模式将 token 放在 params base64 数组首位，故鉴权需解码 params 校验。
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	method := q.Get("method")
	if method == "" {
		writeError(w, nil, errInvalidReq)
		return
	}

	idStr := q.Get("id")
	var id json.RawMessage
	if idStr != "" {
		id = json.RawMessage(`"` + idStr + `"`)
	}

	// params: base64 编码的 JSON 数组，缺失时视为空数组
	paramsRaw := q.Get("params")
	var params json.RawMessage
	if paramsRaw != "" {
		decoded, err := base64.StdEncoding.DecodeString(paramsRaw)
		if err != nil {
			writeError(w, id, errParams)
			return
		}
		params = json.RawMessage(decoded)
	} else {
		params = json.RawMessage(`[]`)
	}

	// 鉴权：URL query token / Authorization header / params 数组首位 token（aria2 GET 模式）
	if s.secret != "" {
		ok := s.authenticate(r)
		if !ok {
			if tok := paramsToken(params); tok != "" {
				ok = tok == "token:"+s.secret || tok == s.secret
			}
		}
		if !ok {
			writeError(w, id, &RPCError{Code: 1, Message: "Unauthorized"})
			return
		}
	}

	// 移除 params[0] 的 token（若存在），避免被当作业务参数
	params = stripToken(params)

	req := Request{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}
	resp := s.dispatch(req)
	writeJSON(w, resp)
}

// ============================== WebSocket (stdlib, 无外部依赖) ==============================

var wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsClient 一个已建立的 WebSocket 连接。
// 读循环与通知广播可能来自不同 goroutine，故所有写入必须经由 mu 串行化，
// 否则会写出交错的帧。
type wsClient struct {
	conn net.Conn
	mu   sync.Mutex
}

func (c *wsClient) send(payload []byte, opcode byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return writeWSFrame(c.conn, payload, opcode)
}

// wsNotification aria2 通知帧（无 id 字段，见 aria2 手册 Notifications 一节）。
type wsNotification struct {
	JSONRPC string              `json:"jsonrpc"`
	Method  string              `json:"method"`
	Params  []map[string]string `json:"params"`
}

// Notify 向所有已认证的 WebSocket 连接广播 aria2 通知。
//
// AriaNg 的 WebSocket 客户端只在本地注册回调（aria2WebSocketRpcService.on），
// 并不发送订阅请求，因此这里无条件广播；通知只含 gid，不含密钥。
// 由 task.Manager 在状态迁移时回调。
func (s *Server) Notify(method, gid string) {
	payload, err := json.Marshal(wsNotification{
		JSONRPC: "2.0",
		Method:  method,
		Params:  []map[string]string{{"gid": gid}},
	})
	if err != nil {
		return
	}

	s.wsMu.RLock()
	clients := make([]*wsClient, 0, len(s.wsClients))
	for c := range s.wsClients {
		clients = append(clients, c)
	}
	s.wsMu.RUnlock()

	for _, c := range clients {
		if err := c.send(payload, 1); err != nil {
			log.Printf("[RPC-ws] notify %s: %v", method, err)
			s.unregisterWS(c)
		}
	}
}

func (s *Server) registerWS(c *wsClient) {
	s.wsMu.Lock()
	s.wsClients[c] = struct{}{}
	s.wsMu.Unlock()
}

func (s *Server) unregisterWS(c *wsClient) {
	s.wsMu.Lock()
	delete(s.wsClients, c)
	s.wsMu.Unlock()
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// WebSocket 握手
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "missing Sec-WebSocket-Key", http.StatusBadRequest)
		return
	}
	hash := sha1.Sum([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(hash[:])

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()

	// 发送 101 响应 (直接写 conn，免 bufio 依赖)
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n" +
		"\r\n"
	conn.Write([]byte(resp))

	client := &wsClient{conn: conn}
	// 消息循环
	buf := make([]byte, 1<<20)
	authed := s.secret == "" // 无 secret 则跳过认证
	if authed {
		s.registerWS(client)
		defer s.unregisterWS(client)
	}

	for {
		// 读取 WebSocket 帧
		msg, op, err := readWSFrame(conn, buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("[RPC-ws] read: %v", err)
			}
			return
		}

		// 仅处理文本帧
		if op != 1 { // text frame
			if op == 8 { // close frame
				log.Printf("[RPC-ws] client closed")
				return
			}
			if op == 9 { // ping → pong
				// 未认证时不允许通过 ping 帧保持连接
				if !authed {
					log.Printf("[RPC-ws] ping before auth, closing")
					return
				}
				_ = client.send(msg, 10) // pong
				continue
			}
			// 未认证时收到非文本帧，关闭连接
			if !authed {
				log.Printf("[RPC-ws] non-text frame before auth (opcode=%d), closing", op)
				return
			}
			log.Printf("[RPC-ws] ignoring opcode=%d len=%d", op, len(msg))
			continue
		}

		// WebSocket 层认证：首条消息必须包含有效 token
		if !authed {
			if tok := extractToken(msg); tok == "token:"+s.secret || tok == s.secret {
				authed = true
				s.registerWS(client)
				defer s.unregisterWS(client)
			} else {
				log.Printf("[RPC-ws] auth failed, closing")
				_ = client.send([]byte(`{"jsonrpc":"2.0","error":{"code":1,"message":"Unauthorized"},"id":null}`), 1)
				return
			}
		}

		// 解析 JSON-RPC（支持单条和批量）
		msg = bytes.TrimLeft(msg, " \t\r\n")
		if len(msg) > 0 && msg[0] == '[' {
			// 批量请求
			var reqs []Request
			if err := json.Unmarshal(msg, &reqs); err != nil {
				log.Printf("[RPC-ws] batch json: %v", err)
				continue
			}
			responses := make([]Response, 0, len(reqs))
			for _, req := range reqs {
				req.Params = stripToken(req.Params)
				responses = append(responses, s.dispatch(req))
			}
			respBytes, _ := json.Marshal(responses)
			if err := client.send(respBytes, 1); err != nil {
				log.Printf("[RPC-ws] write: %v", err)
				return
			}
		} else {
			// 单条请求
			var req Request
			if err := json.Unmarshal(msg, &req); err != nil {
				log.Printf("[RPC-ws] json: %v", err)
				continue
			}
			req.Params = stripToken(req.Params)

			resp := s.dispatch(req)
			respBytes, _ := json.Marshal(resp)
			if err := client.send(respBytes, 1); err != nil {
				log.Printf("[RPC-ws] write: %v", err)
				return
			}
		}
	}
}

// readWSFrame 读取一个 WebSocket 帧，返回 payload、opcode 和 error
func readWSFrame(conn net.Conn, buf []byte) ([]byte, byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, 0, err
	}
	opcode := header[0] & 0x0F
	masked := header[1]&0x80 != 0
	length := uint64(header[1] & 0x7F)

	switch {
	case length == 126:
		ext := make([]byte, 2)
		io.ReadFull(conn, ext)
		length = uint64(binary.BigEndian.Uint16(ext))
	case length == 127:
		ext := make([]byte, 8)
		io.ReadFull(conn, ext)
		length = binary.BigEndian.Uint64(ext)
	}

	var mask [4]byte
	if masked {
		io.ReadFull(conn, mask[:])
	}

	if length > uint64(len(buf)) {
		return nil, 0, fmt.Errorf("frame too large: %d", length)
	}
	payload := buf[:length]
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, 0, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return payload, opcode, nil
}

// writeWSFrame 写入一个 WebSocket 文本帧
func writeWSFrame(conn net.Conn, payload []byte, opcode byte) error {
	n := len(payload)
	frame := make([]byte, 0, n+14)

	frame = append(frame, 0x80|opcode) // FIN + opcode
	switch {
	case n < 126:
		frame = append(frame, byte(n))
	case n < 1<<16:
		frame = append(frame, 126)
		frame = append(frame, byte(n>>8), byte(n))
	default:
		frame = append(frame, 127)
		for i := 7; i >= 0; i-- {
			frame = append(frame, byte(n>>(i*8)))
		}
	}
	frame = append(frame, payload...)
	_, err := conn.Write(frame)
	return err
}

// ============================== 鉴权 ==============================

func (s *Server) authenticate(r *http.Request) bool {
	// token:secret 参数 (aria2 兼容)
	if tok := r.URL.Query().Get("token"); tok != "" {
		return tok == "token:"+s.secret || tok == s.secret
	}
	// Authorization Bearer header
	if auth := r.Header.Get("Authorization"); auth != "" {
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
			v := parts[1]
			return v == s.secret || v == "token:"+s.secret
		}
	}
	return false
}

// authenticateBody 从 JSON body 的 params[0] 提取 token
func (s *Server) authenticateBody(body []byte) bool {
	tok := extractToken(body)
	return tok == "token:"+s.secret || tok == s.secret
}

// extractToken 从 JSON-RPC 消息中提取 token。
func extractToken(body []byte) string {
	var req struct {
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	return paramsToken(req.Params)
}

// paramsToken 从已解码的 params 数组提取 token。
// 普通方法的 token 位于 params[0]；而 system.multicall 顶层不带 token，
// token 位于各子调用的 params[0]（aria2 对 system.* 方法不追加顶层 token，
// AriaNg 的 buildRequestContext 亦如此），故需下钻一层。
func paramsToken(raw json.RawMessage) string {
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil || len(arr) == 0 {
		return ""
	}
	var tok string
	if err := json.Unmarshal(arr[0], &tok); err == nil && tok != "" {
		return tok
	}
	// params[0] 非字符串：按 multicall 的调用列表解析
	var calls []struct {
		Params []json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(arr[0], &calls); err != nil {
		return ""
	}
	for _, c := range calls {
		if len(c.Params) == 0 {
			continue
		}
		if err := json.Unmarshal(c.Params[0], &tok); err == nil && tok != "" {
			return tok
		}
	}
	return ""
}

// ============================== 请求处理 ==============================

func (s *Server) handleSingle(w http.ResponseWriter, body []byte) {
	var req Request
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, nil, errParse)
		return
	}
	req.Params = stripToken(req.Params)

	resp := s.dispatch(req)
	writeJSON(w, resp)
}

func (s *Server) handleBatch(w http.ResponseWriter, body []byte) {
	var reqs []Request
	if err := json.Unmarshal(body, &reqs); err != nil {
		writeError(w, nil, errParse)
		return
	}

	responses := make([]Response, 0, len(reqs))
	for _, req := range reqs {
		req.Params = stripToken(req.Params)
		responses = append(responses, s.dispatch(req))
	}
	writeJSON(w, responses)
}

// stripToken 从 params 数组中移除首位 token（aria2 兼容）
func stripToken(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil || len(arr) == 0 {
		return raw
	}
	var tok string
	if err := json.Unmarshal(arr[0], &tok); err != nil {
		return raw
	}
	if !strings.HasPrefix(tok, "token:") {
		return raw
	}
	// 移除 token，剩余元素重新打包
	rest, _ := json.Marshal(arr[1:])
	return json.RawMessage(rest)
}

func (s *Server) dispatch(req Request) Response {
	id := parseID(req.ID)

	h, ok := Dispatch(req.Method)
	if !ok {
		return newError(id, errMethod)
	}

	result, err := h(req.Params, s.manager)
	if err != nil {
		if rpcErr, ok := err.(*RPCError); ok {
			return newError(id, rpcErr)
		}
		log.Printf("[RPC] method %s: %v", req.Method, err)
		return newError(id, errInternal)
	}

	return newResponse(id, result)
}

// ============================== HTTP 响应辅助 ==============================

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, id interface{}, rpcErr *RPCError) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK) // JSON-RPC 错误仍返回 200
	json.NewEncoder(w).Encode(newError(id, rpcErr))
}

// ============================== CORS 中间件 ==============================

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

		// 回显浏览器请求的 headers（兼容所有 AriaNg 版本）
		if reqH := r.Header.Get("Access-Control-Request-Headers"); reqH != "" {
			w.Header().Set("Access-Control-Allow-Headers", reqH)
		} else {
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
