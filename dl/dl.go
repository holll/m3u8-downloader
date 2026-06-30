package dl

import (
	"log"
	"os"
	"time"

	"m3u8-downloader/util"

	"github.com/levigross/grequests"
)

// ============================== 包级 Logger ==============================

// Log 包级别 logger，可供外部替换
var Log = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

// ============================== Config ==============================

// Config 下载器配置
type Config struct {
	M3U8URL    string
	OutputDir  string
	MaxWorkers int
	HostType   string
	AutoClear  bool
	Cookie     string
	Insecure   bool
}

// ============================== Downloader ==============================

// Downloader HLS 下载器
type Downloader struct {
	m3u8URL     string
	m3u8Host    string
	m3u8Body    string
	segments    []Segment
	isFmp4      bool
	isEncrypted bool
	outputDir   string
	outputFile  string
	maxWorkers  int
	autoClear   bool
	hostType    string
	reqOpts     *grequests.RequestOptions
	progress    *ProgressTracker
}

// New 创建下载器实例
func New(cfg Config) *Downloader {
	d := &Downloader{
		m3u8URL:    cfg.M3U8URL,
		outputDir:  cfg.OutputDir,
		outputFile: cfg.OutputDir + ".mp4",
		maxWorkers: cfg.MaxWorkers,
		hostType:   cfg.HostType,
		autoClear:  cfg.AutoClear,
	}
	d.initRequestOptions(cfg.Cookie, cfg.Insecure)
	return d
}

// --- 状态查询方法 ---

// SegmentCount 返回解析到的切片数量
func (d *Downloader) SegmentCount() int { return len(d.segments) }

// IsFmp4 返回是否为 fMP4 格式
func (d *Downloader) IsFmp4() bool { return d.isFmp4 }

// IsEncrypted 返回是否有加密切片
func (d *Downloader) IsEncrypted() bool { return d.isEncrypted }

// OutputFile 返回输出文件路径
func (d *Downloader) OutputFile() string { return d.outputFile }

// AutoClear 返回是否自动清理临时文件
func (d *Downloader) AutoClear() bool { return d.autoClear }

// OutputDir 返回临时下载目录
func (d *Downloader) OutputDir() string { return d.outputDir }

// --- 请求初始化 ---

func (d *Downloader) initRequestOptions(cookie string, insecure bool) {
	opts := &grequests.RequestOptions{
		UserAgent:      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
		RequestTimeout: 30 * time.Second,
		Headers: map[string]string{
			"Connection":      "keep-alive",
			"Accept":          "*/*",
			"Accept-Encoding": "*",
			"Accept-Language": "zh-CN,zh;q=0.9, en;q=0.8, de;q=0.7, *;q=0.5",
			"Referer":         util.GetHost(d.m3u8URL, "v2"),
		},
	}
	if insecure {
		opts.InsecureSkipVerify = true
	}
	if cookie != "" {
		opts.Headers["Cookie"] = cookie
	}
	d.reqOpts = opts
}
