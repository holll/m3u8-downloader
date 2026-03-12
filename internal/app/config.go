package app

import (
	"flag"
	"log"
	"os"
	"sync"
	"time"

	"github.com/levigross/grequests"
)

// 全局常量与默认参数配置。
const (
	// HEAD_TIMEOUT 请求头超时时间
	HEAD_TIMEOUT = 5 * time.Second
	// TS_TIMEOUT ts分片下载超时时间
	TS_TIMEOUT = 20 * time.Second
	// PROGRESS_WIDTH 进度条长度
	PROGRESS_WIDTH = 20
	// TS_NAME_TEMPLATE ts视频片段命名规则
	TS_NAME_TEMPLATE = "%05d.ts"
)

var (
	// 命令行参数
	urlFlag       = flag.String("u", "", "m3u8下载地址(http(s)://url/xx/xx/index.m3u8)")
	nFlag         = flag.Int("n", 24, "num:下载线程数(默认24)")
	jFlag         = flag.Int("j", 1, "jobNum:并行下载任务数(默认1, 仅API模式生效)")
	htFlag        = flag.String("ht", "v1", "hostType:设置getHost的方式(v1: `http(s):// + url.Host + filepath.Dir(url.Path)`; v2: `http(s)://+ u.Host`")
	oFlag         = flag.String("o", "movie", "movieName:自定义文件名(默认为movie)不带后缀")
	cFlag         = flag.String("c", "", "cookie:自定义请求cookie")
	rFlag         = flag.Bool("r", true, "autoClear:是否自动清除ts文件")
	sFlag         = flag.Int("s", 0, "InsecureSkipVerify:是否允许不安全的请求(默认0)")
	spFlag        = flag.String("sp", "", "savePath:文件保存的绝对路径(默认为当前路径,建议默认值)")
	apiFlag       = flag.String("api-listen", "", "apiListen:aria2风格JSON-RPC地址(例如 :6800)")
	rpcSecretFlag = flag.String("rpc-secret", "", "rpcSecret:aria2 rpc鉴权密钥(API模式必填)")

	logger *log.Logger
	ro     = grequests.RequestOptions{
		UserAgent:      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
		RequestTimeout: HEAD_TIMEOUT,
		Headers: map[string]string{
			"Connection":      "keep-alive",
			"Accept":          "*/*",
			"Accept-Encoding": "*",
			"Accept-Language": "zh-CN,zh;q=0.9, en;q=0.8, de;q=0.7, *;q=0.5",
		},
	}
)

// DownloadJob 描述一次下载任务的完整参数。
type DownloadJob struct {
	M3U8URL       string
	MaxGoroutines int
	HostType      string
	MovieName     string
	OutputName    string
	Cookie        string
	AutoClear     bool
	Insecure      int
	SavePath      string
}

type TaskStatus struct {
	GID               string `json:"gid"`
	Status            string `json:"status"`
	Result            string `json:"result,omitempty"`
	Error             string `json:"error,omitempty"`
	TotalSegments     int    `json:"totalSegments,omitempty"`
	CompletedSegments int    `json:"completedSegments,omitempty"`
}

type DownloadManager struct {
	limiter   chan struct{}
	mu        sync.RWMutex
	tasks     map[string]*TaskStatus
	rpcSecret string
}

// TsInfo 用于保存 ts 文件的下载地址和文件名
type TsInfo struct {
	Name string
	Url  string
}

type ProgressFunc func(done, total int)

func init() {
	logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}
