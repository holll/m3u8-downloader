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
	reqCount int64 // atomic request counter
}

// NewServer 创建服务实例
func NewServer(addr string, mgr *task.Manager, secret string) *Server {
	return &Server{
		manager: mgr,
		secret:  secret,
		addr:    addr,
	}
}

// Start 启动 HTTP 服务，阻塞直到 Stop()
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/jsonrpc", s.handleJSONRPC)

	s.hs = &http.Server{
		Addr:    s.addr,
		Handler: withCORS(mux),
	}
	fmt.Printf("[RPC] listening on http://%s/jsonrpc\n", s.addr)
	return s.hs.ListenAndServe()
}

// Stop 优雅关闭
func (s *Server) Stop() error {
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
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	// 鉴权
	if s.secret != "" && !s.authenticate(r) {
		writeError(w, nil, &RPCError{Code: 1, Message: "Unauthorized"})
		return
	}

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
			writeError(w, nil, errParams)
			return
		}
		params = json.RawMessage(decoded)
	} else {
		params = json.RawMessage(`[]`)
	}

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

	// 消息循环
	buf := make([]byte, 1<<20)
	authed := s.secret == "" // 无 secret 则跳过认证

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
				writeWSFrame(conn, msg, 10) // pong
				continue
			}
			log.Printf("[RPC-ws] ignoring opcode=%d len=%d", op, len(msg))
			continue
		}

		// WebSocket 层认证：首条消息必须包含有效 token
		if !authed {
			if tok := extractToken(msg); tok == "token:"+s.secret || tok == s.secret {
				authed = true
			} else {
				log.Printf("[RPC-ws] auth failed, closing")
				writeWSFrame(conn, []byte(`{"jsonrpc":"2.0","error":{"code":1,"message":"Unauthorized"},"id":null}`), 1)
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
			if err := writeWSFrame(conn, respBytes, 1); err != nil {
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
			if err := writeWSFrame(conn, respBytes, 1); err != nil {
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

// extractToken 从 JSON-RPC 消息的 params[0] 提取 token 字符串
func extractToken(body []byte) string {
	var req struct {
		Params []json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(body, &req); err != nil || len(req.Params) == 0 {
		return ""
	}
	var tok string
	json.Unmarshal(req.Params[0], &tok)
	return tok
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
