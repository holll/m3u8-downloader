// Package rpc 提供 aria2 兼容的 JSON-RPC 2.0 服务端实现。
package rpc

import (
	"encoding/json"
	"fmt"
)

// ============================== JSON-RPC 2.0 类型 ==============================

// Request JSON-RPC 请求
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id"`
}

// Response JSON-RPC 响应
type Response struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

// RPCError JSON-RPC 错误
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

// 标准 JSON-RPC 2.0 错误码
const (
	ErrParse      = -32700
	ErrInvalidReq = -32600
	ErrMethod     = -32601
	ErrParams     = -32602
	ErrInternal   = -32603
)

// 常用错误实例
var (
	errParse      = &RPCError{Code: ErrParse, Message: "Parse error"}
	errInvalidReq = &RPCError{Code: ErrInvalidReq, Message: "Invalid Request"}
	errMethod     = &RPCError{Code: ErrMethod, Message: "Method not found"}
	errParams     = &RPCError{Code: ErrParams, Message: "Invalid params"}
	errInternal   = &RPCError{Code: ErrInternal, Message: "Internal error"}
)

// ============================== 辅助 ==============================

// newResponse 构造成功响应
func newResponse(id interface{}, result interface{}) Response {
	return Response{
		JSONRPC: "2.0",
		Result:  result,
		ID:      id,
	}
}

// newError 构造错误响应
func newError(id interface{}, err *RPCError) Response {
	return Response{
		JSONRPC: "2.0",
		Error:   err,
		ID:      id,
	}
}

// parseID 尝试将 json.RawMessage 的 ID 解析为可用类型
func parseID(raw json.RawMessage) interface{} {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	// 尝试解析为字符串
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	// 尝试解析为数字
	var n json.Number
	if json.Unmarshal(raw, &n) == nil {
		return n
	}
	return string(raw)
}

// ============================== 请求入参解析辅助 ==============================

// parseParams 将 json.RawMessage params 解析到目标结构体
// params 必须是 JSON 数组，取第一个元素
func parseParams(raw json.RawMessage, v interface{}) error {
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return fmt.Errorf("%w: expected array", errParams)
	}
	if len(arr) == 0 {
		return fmt.Errorf("%w: empty params", errParams)
	}
	if err := json.Unmarshal(arr[0], v); err != nil {
		return fmt.Errorf("%w: %v", errParams, err)
	}
	return nil
}

// parseParams2 解析 params 数组的前两个元素
func parseParams2(raw json.RawMessage, v1, v2 interface{}) error {
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return fmt.Errorf("%w: expected array", errParams)
	}
	if len(arr) < 2 {
		return fmt.Errorf("%w: need 2 params", errParams)
	}
	if err := json.Unmarshal(arr[0], v1); err != nil {
		return fmt.Errorf("%w: %v", errParams, err)
	}
	if err := json.Unmarshal(arr[1], v2); err != nil {
		return fmt.Errorf("%w: %v", errParams, err)
	}
	return nil
}
