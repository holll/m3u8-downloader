package dl

import (
	"crypto/tls"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	autoNamed   bool // AutoName 是否命中 PROGRAM-DATE-TIME（用于区分占位路径与真实命名）
	maxWorkers  int
	autoClear   bool
	autoName    bool
	hostType    string
	quiet       bool // 不打印进度条
	maxRetry    int  // 单分片最大重试次数
	reqOpts     *grequests.RequestOptions
	progress    *ProgressTracker

	// 回调 (供外部监下载进度)
	onBytes       func(int64)
	onProgress    func(completed, total int64)
	onSegmentDone func(index int) // 单个切片就绪（新下载或已存在）

	// 首个分片失败原因（downloadSegments 并发写入，只记第一次）
	segErrOnce sync.Once
	segErr     error

	inflight int64 // 正在处理的切片数（atomic，供 aria2 connections 字段上报）

	// key 缓存：URI → keyData，并发安全
	keyCache   map[string][]byte
	keyCacheMu sync.Mutex
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
		keyCache:   make(map[string][]byte),
	}
	if d.maxWorkers <= 0 {
		// 兜底：maxWorkers 为 0 会让 downloadSegments 的 limiter 容量为 0，
		// 阻塞式发送导致所有 goroutine 永久卡死。
		d.maxWorkers = DefaultMaxWorkers
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

// OnSegmentDone 注册单切片就绪回调，参数为该切片的 Segment.Index。
// 与 OnProgress 同时触发；断点续传时已存在的切片同样会回调，便于外部重建完成位图。
func (d *Downloader) OnSegmentDone(fn func(index int)) { d.onSegmentDone = fn }

// Inflight 返回当前正在处理的切片数（即实际并发度）。
// 供 aria2 的 connections 字段使用：它随剩余切片数下降而回落，而非恒等于配置的线程数。
func (d *Downloader) Inflight() int { return int(atomic.LoadInt64(&d.inflight)) }

// SegmentTotal 返回解析到的 total segments (供外部读取)
func (d *Downloader) SegmentTotal() int64 { return int64(len(d.segments)) }

// SetOutputFile 覆盖输出文件路径 (供 RPC 模式按选项设置)
func (d *Downloader) SetOutputFile(path string) { d.outputFile = path }

// Segments 返回解析到的切片列表的只读副本
func (d *Downloader) Segments() []Segment {
	cp := make([]Segment, len(d.segments))
	copy(cp, d.segments)
	return cp
}

// AutoName 从流的 PROGRAM-DATE-TIME 元数据自动生成输出文件名。
// baseDir 是最终输出文件所在的目录（通常为当前工作目录）。
// 仅在 autoName 为 true 且解析到了有效时间戳时生效；未命中时 outputFile
// 保持创建时的占位路径，可通过 AutoNamed() 判断是否命中。
func (d *Downloader) AutoName(baseDir string) {
	if !d.autoName {
		return
	}
	for _, seg := range d.segments {
		if !seg.ProgramDateTime.IsZero() {
			name := seg.ProgramDateTime.Format("20060102_150405")
			d.outputFile = filepath.Join(baseDir, name+".mp4")
			d.autoNamed = true
			Log.Printf("[info] 自动识别文件名: %s.mp4", name)
			return
		}
	}
	Log.Println("[info] 未检测到 PROGRAM-DATE-TIME，使用默认文件名")
}

// AutoNamed 返回 AutoName 是否命中有效 PROGRAM-DATE-TIME 并设置了输出文件名。
func (d *Downloader) AutoNamed() bool { return d.autoNamed }

// --- 请求初始化 ---

func (d *Downloader) initRequestOptions(cookie string, insecure bool) {
	transport := &http.Transport{
		// 读取 HTTP_PROXY / HTTPS_PROXY / NO_PROXY 环境变量（start.bat 中设置的代理由此生效）。
		// 环境变量未设置时返回 nil，等价于直连；NO_PROXY 可用于放行不走代理的地址。
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: d.maxWorkers + 2,
		MaxConnsPerHost:     d.maxWorkers + 5,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableCompression:  true,
	}
	if insecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	referer, _ := util.GetHost(d.m3u8URL, "v2")
	opts := &grequests.RequestOptions{
		HTTPClient: client,
		UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
		Headers: map[string]string{
			"Accept":          "*/*",
			"Accept-Language": "zh-CN,zh;q=0.9, en;q=0.8, de;q=0.7, *;q=0.5",
			"Referer":         referer,
		},
	}
	if cookie != "" {
		opts.Headers["Cookie"] = cookie
	}
	d.reqOpts = opts
}
