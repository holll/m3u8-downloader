package task

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"

	"m3u8-downloader/dl"
)

// aria2 错误码（取值见 aria2 手册 EXIT STATUS；AriaNg 的 aria2Errors 表据此
// 显示本地化描述，未知码值不显示描述）。
const (
	errCodeUnknown          = "1"  // unknown error
	errCodeResourceNotFound = "3"  // resource not found
	errCodeNetworkProblem   = "6"  // network problem
	errCodeNotEnoughSpace   = "9"  // not enough disk space
	errCodeFileOpenFailed   = "15" // file open failed
	errCodeIOError          = "17" // I/O error
	errCodeDirCreateFailed  = "18" // directory creation failed
	errCodeNameResolution   = "19" // name resolution failed
	errCodeHTTPBadResponse  = "22" // HTTP response header was bad or unexpected
	errCodeAuthFailed       = "24" // HTTP authorization failed
)

// aria2ErrorCode 将底层错误分类为 aria2 的 errorCode。
//
// 分类依据 dl 包的结构化错误与标准库错误类型，不做错误文案匹配。
// 无法归类时返回 "1"（unknown），与 aria2 一致。
func aria2ErrorCode(err error) string {
	if err == nil {
		return errCodeUnknown
	}

	var httpErr *dl.HTTPStatusError
	if errors.As(err, &httpErr) {
		switch {
		case httpErr.Status == 401 || httpErr.Status == 403 || httpErr.Status == 407:
			return errCodeAuthFailed
		case httpErr.Status == 404 || httpErr.Status == 410:
			return errCodeResourceNotFound
		default:
			return errCodeHTTPBadResponse
		}
	}

	// 域名解析失败（aria2 单列此码，且与普通网络故障区分）
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return errCodeNameResolution
	}

	if errors.Is(err, syscall.ENOSPC) {
		return errCodeNotEnoughSpace
	}

	// MkdirAll 失败（输出目录/临时目录不可建）先于权限判断，
	// 因为「建目录被拒」比泛化的「文件打开失败」更精确
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && pathErr.Op == "mkdir" {
		return errCodeDirCreateFailed
	}

	if errors.Is(err, os.ErrPermission) {
		return errCodeFileOpenFailed
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return errCodeNetworkProblem
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return errCodeNetworkProblem
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errCodeNetworkProblem
	}

	return errCodeUnknown
}
