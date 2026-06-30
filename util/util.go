// Package util 提供通用工具函数。
package util

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// ============================== URL ==============================

// GetHost 从 m3u8 URL 提取基础路径
// ht="v1" → scheme://host + path 目录
// ht="v2" → scheme://host
func GetHost(Url, ht string) string {
	u, err := url.Parse(Url)
	if err != nil {
		log.Fatal(err)
	}
	switch ht {
	case "v1":
		return u.Scheme + "://" + u.Host + filepath.Dir(u.EscapedPath())
	case "v2":
		return u.Scheme + "://" + u.Host
	}
	return ""
}

// ============================== 进度条 ==============================

// DrawProgressBar 绘制终端进度条
func DrawProgressBar(prefix string, proportion float32, width int, suffix ...string) {
	pos := int(proportion * float32(width))
	s := fmt.Sprintf("[%s] %s%*s %6.2f%% \t%s",
		prefix, strings.Repeat("■", pos), width-pos, "", proportion*100, strings.Join(suffix, ""))
	fmt.Print("\r" + s)
}

// ============================== 文件 ==============================

// PathExists 检查路径是否存在
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
