package dl

import (
	"log"
	"os"
	"path/filepath"
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
	AutoName   bool
	Cookie     string
	Insecure   bool
	Quiet      bool // 静默模式：不打印终端进度条（Server 模式）
	MaxRetry   int  // 单分片最大重试次数（0 或不设置 = 无限）
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
	autoName    bool
	hostType    string
	quiet       bool // 不打印进度条
	maxRetry    int  // 单分片最大重试次数
	reqOpts     *grequests.RequestOptions
	progress    *ProgressTracker

	// 回调 (供外部监下载进度)
	onBytes    func(int64)
	onProgress func(completed, total int64)
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
		autoName:   cfg.AutoName,
		quiet:      cfg.Quiet,
		maxRetry:   cfg.MaxRetry,
	}
	if d.maxRetry <= 0 {
		d.maxRetry = 5
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

// OnBytes 注册字节回调 (每次 HTTP 响应后触发)
func (d *Downloader) OnBytes(fn func(int64)) { d.onBytes = fn }

// OnProgress 注册进度回调 (每完成一个 segment 触发)
func (d *Downloader) OnProgress(fn func(completed, total int64)) { d.onProgress = fn }

// SegmentTotal 返回解析到的 total segments (供外部读取)
func (d *Downloader) SegmentTotal() int64 { return int64(len(d.segments)) }

// SetOutputFile 覆盖输出文件路径 (供 RPC 模式按选项设置)
func (d *Downloader) SetOutputFile(path string) { d.outputFile = path }

// AutoName 从流的 PROGRAM-DATE-TIME 元数据自动生成输出文件名。
// baseDir 是最终输出文件所在的目录（通常为当前工作目录）。
// 仅在 autoName 为 true 且解析到了有效时间戳时生效。
func (d *Downloader) AutoName(baseDir string) {
	if !d.autoName {
		return
	}
	for _, seg := range d.segments {
		if !seg.ProgramDateTime.IsZero() {
			name := seg.ProgramDateTime.Format("20060102_150405")
			d.outputFile = filepath.Join(baseDir, name+".mp4")
			Log.Printf("[info] 自动识别文件名: %s.mp4", name)
			return
		}
	}
	Log.Println("[info] 未检测到 PROGRAM-DATE-TIME，使用默认文件名")
}

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
