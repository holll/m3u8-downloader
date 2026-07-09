// Package task 提供 aria2 兼容的任务管理功能。
package task

import (
	"encoding/json"
	"strconv"
	"time"
)

// ============================== 状态枚举 ==============================

// Status 任务状态 (aria2 兼容)
type Status int

const (
	StatusWaiting  Status = iota // 排队等待槽位
	StatusActive                 // 正在下载
	StatusPaused                 // 用户暂停
	StatusError                  // 下载失败
	StatusComplete               // 下载完成（已合并）
	StatusRemoved                // 已删除
)

func (s Status) String() string {
	switch s {
	case StatusWaiting:
		return "waiting"
	case StatusActive:
		return "active"
	case StatusPaused:
		return "paused"
	case StatusError:
		return "error"
	case StatusComplete:
		return "complete"
	case StatusRemoved:
		return "removed"
	}
	return "unknown"
}

// MarshalJSON 序列化为 aria2 兼容的字符串
func (s Status) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// ============================== aria2 响应类型 ==============================

// FileInfo aria2 文件信息
type FileInfo struct {
	Index           string    `json:"index"`
	Path            string    `json:"path"`
	Length          string    `json:"length"`
	CompletedLength string    `json:"completedLength"`
	Selected        string    `json:"selected"`
	URIs            []URIInfo `json:"uris"`
}

// URIInfo 文件的 URI 信息
type URIInfo struct {
	URI    string `json:"uri"`
	Status string `json:"status"`
}

// TaskStatus aria2 tellStatus 返回的任务快照
type TaskStatus struct {
	GID             string     `json:"gid"`
	Status          string     `json:"status"`
	TotalLength     string     `json:"totalLength"`
	CompletedLength string     `json:"completedLength"`
	DownloadSpeed   string     `json:"downloadSpeed"`
	UploadSpeed     string     `json:"uploadSpeed"`
	UploadLength    string     `json:"uploadLength"`
	Connections     string     `json:"connections"`
	ErrorCode       string     `json:"errorCode"`
	ErrorMessage    string     `json:"errorMessage,omitempty"`
	Dir             string     `json:"dir"`
	Files           []FileInfo `json:"files"`
	Bittorrent      struct{}   `json:"bittorrent"`
	InfoHash        string     `json:"infoHash"`
	NumPieces       string     `json:"numPieces"`
	PieceLength     string     `json:"pieceLength"`
	FollowedBy      []string   `json:"followedBy"`
	BelongsTo       string     `json:"belongsTo"`
}

// GlobalStat aria2.getGlobalStat 返回
type GlobalStat struct {
	DownloadSpeed   string `json:"downloadSpeed"`
	UploadSpeed     string `json:"uploadSpeed"`
	NumActive       string `json:"numActive"`
	NumWaiting      string `json:"numWaiting"`
	NumStopped      string `json:"numStopped"`
	NumStoppedTotal string `json:"numStoppedTotal"`
}

// VersionInfo aria2.getVersion 返回
type VersionInfo struct {
	Version  string   `json:"version"`
	Features []string `json:"enabledFeatures"`
}

// SessionInfo aria2.getSessionInfo 返回
type SessionInfo struct {
	ID string `json:"sessionId"`
}

// 辅助：int64 转 aria2 字符串格式
func itoa(v int64) string {
	if v < 0 {
		return "0"
	}
	return json.Number(itoaRaw(v)).String()
}

func itoaRaw(v int64) string {
	return formatInt(v)
}

func formatInt(v int64) string {
	if v == 0 {
		return "0"
	}
	s := ""
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		s = string(rune('0'+v%10)) + s
		v /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}

// ============================== 任务选项 ==============================

// Options addUri 传入的任务选项
type Options struct {
	Dir        string `json:"dir"`
	Out        string `json:"out"`
	Cookie     string `json:"cookie,omitempty"`
	Header     string `json:"header,omitempty"` // 暂未使用
	MaxWorkers int    `json:"max-connection-per-server,omitempty"`
	MaxRetry   int    `json:"max-retry,omitempty"` // 分片重试次数（0 = 取默认值 5）
}

// ============================== 会话持久化 ==============================

// SessionEntry 会话文件中的单个任务条目，用于重启后恢复。
type SessionEntry struct {
	GID               string `json:"gid"`
	URL               string `json:"url"`
	Dir               string `json:"dir"`
	Out               string `json:"out"`
	Cookie            string `json:"cookie,omitempty"`
	MaxWorkers        int    `json:"maxWorkers"`
	Status            string `json:"status"`
	TempDir           string `json:"tempDir,omitempty"`
	TotalSegments     int64  `json:"totalSegments"`
	CompletedSegments int64  `json:"completedSegments"`
	BytesReceived     int64  `json:"bytesReceived"`
}

// ============================== 进度持久化 ==============================

// ProgressFile 单个任务的断点续传进度文件 ({gid}.progress)。
// 位于下载临时目录，每完成一批切片后更新。
type ProgressFile struct {
	GID               string    `json:"gid"`
	TotalSegments     int64     `json:"totalSegments"`
	CompletedSegments int64     `json:"completedSegments"`
	BytesReceived     int64     `json:"bytesReceived"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

// ============================== 辅助 ==============================

// toInt 从 interface{} 提取 int 值（支持 float64, json.Number, string）。
func toInt(v interface{}) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case json.Number:
		n, _ := val.Int64()
		return int(n)
	case string:
		n, _ := strconv.Atoi(val)
		return n
	default:
		return 0
	}
}

// ============================== 时间 ==============================

var now = time.Now // 允许测试中替换
