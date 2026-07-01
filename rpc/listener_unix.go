//go:build !windows

package rpc

import (
	"context"
	"net"
	"syscall"
)

// createListener 创建 TCP listener，启用 SO_REUSEADDR。
// Unix 上允许端口在 TIME_WAIT 后立即复用。
func createListener(addr string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}
	return lc.Listen(context.Background(), "tcp", addr)
}
