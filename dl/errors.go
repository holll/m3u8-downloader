package dl

import "fmt"

// HTTPStatusError HTTP 响应异常（非 2xx）。
//
// 单独建模而非直接 fmt.Errorf，是为了让上层能据此分类出 aria2 的 errorCode
// （如 404 → 3 资源不存在、401/403 → 24 鉴权失败），而不是只能拿到一句
// 已格式化的字符串。
type HTTPStatusError struct {
	Status int    // HTTP 状态码
	URL    string // 请求地址（可为空）
}

func (e *HTTPStatusError) Error() string {
	if e.URL != "" {
		return fmt.Sprintf("HTTP %d: %s", e.Status, e.URL)
	}
	return fmt.Sprintf("HTTP %d", e.Status)
}

// newHTTPStatusError 构造 HTTP 状态错误。
func newHTTPStatusError(status int, url string) error {
	return &HTTPStatusError{Status: status, URL: url}
}
