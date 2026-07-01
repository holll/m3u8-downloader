// Package config 提供 aria2 风格的 key=value 配置文件解析。
package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Load 读取配置文件，返回 key → value 映射。
//
// 格式规则（兼容 aria2.conf）：
//   - 每行一条 key=value
//   - # 或 ; 开头为注释
//   - 行尾空格 / 首尾空白被忽略
//   - key 大小写敏感
//   - 空行跳过
func Load(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	cfg := make(map[string]string)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// 跳过空行和注释
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}

		// 分割 key=value
		idx := strings.IndexByte(line, '=')
		if idx < 0 {
			continue
		}

		key := strings.TrimSpace(line[:idx])
		value := strings.TrimSpace(line[idx+1:])

		if key == "" {
			continue
		}

		cfg[key] = value
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	return cfg, nil
}
