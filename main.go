// @author:llychao<lychao_vip@163.com>
// @contributor: Junyi<me@junyi.pw>
// @date:2020-02-18
// @功能:golang m3u8 video Downloader (CLI + aria2 RPC)
// @fix:2026-06-30 — fMP4 support, concurrency fixes, proper IV handling, aria2 RPC server
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"m3u8-downloader/config"
	"m3u8-downloader/dl"
	"m3u8-downloader/rpc"
	"m3u8-downloader/task"
	"m3u8-downloader/util"
)

// ============================== 命令行参数 ==============================

var (
	// CLI 模式
	urlFlag = flag.String("u", "", "m3u8下载地址(http(s)://url/xx/xx/index.m3u8)")
	nFlag   = flag.Int("n", 3, "num:下载线程数(默认3)")
	htFlag  = flag.String("ht", "v1", "hostType: v1/v2")
	oFlag   = flag.String("o", "movie", "movieName:自定义文件名(默认为movie)")
	cFlag   = flag.String("c", "", "cookie:自定义请求cookie")
	rFlag   = flag.Bool("r", true, "autoClear:是否自动清除ts文件")
	sFlag   = flag.Int("s", 0, "InsecureSkipVerify:是否允许不安全的请求")
	spFlag  = flag.String("sp", "", "savePath:文件保存的绝对路径")

	// RPC Server 模式
	rpcPort     = flag.Int("rpc-listen-port", 0, "RPC监听端口(0=CLI模式, 非0=Server模式)")
	rpcSecret   = flag.String("rpc-secret", "", "RPC鉴权token(空=无鉴权)")
	rpcListen   = flag.Bool("rpc-listen-all", false, "监听所有网卡(默认仅localhost)")
	maxDownload = flag.Int("max-concurrent-downloads", 1, "最大同时下载数(Server模式, 默认1)")
	sessionFile = flag.String("session-file", "m3u8.session", "会话文件路径(保存未完成任务, 重启恢复)")
	confPath    = flag.String("conf-path", "", "配置文件路径(key=value格式, CLI参数优先)")
)

func main() {
	Run()
}

// Run 主流程
func Run() {
	// 0. 从命令行参数中提前提取 --conf-path，加载配置文件
	loadConfigFromArgs()

	// 1. 解析命令行参数（CLI 参数优先级高于配置文件）
	flag.Parse()

	if *rpcPort != 0 {
		runServer()
	} else {
		runCLI()
	}
}

// loadConfigFromArgs 从 os.Args 中提取 --conf-path，加载配置文件，
// 并用配置项设置 flag 默认值（后续 flag.Parse() 会用 CLI 参数覆盖）。
func loadConfigFromArgs() {
	cfgPath := peekConfPath(os.Args[1:])
	if cfgPath == "" {
		return
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Printf("[config] 加载失败: %v", err)
		return
	}

	for k, v := range cfg {
		// 不覆盖已从环境变量或其他来源设置的值
		if err := flag.Set(k, v); err != nil {
			log.Printf("[config] 跳过未知配置项: %s=%s", k, v)
		}
	}
	log.Printf("[config] 已从 %s 加载 %d 条配置", cfgPath, len(cfg))
}

// peekConfPath 从参数列表中提取 --conf-path 的值。
// 支持 --conf-path=value 和 --conf-path value 两种格式。
func peekConfPath(args []string) string {
	for i, a := range args {
		// --conf-path=value
		if after, ok := strings.CutPrefix(a, "--conf-path="); ok {
			return after
		}
		// --conf-path value
		if a == "--conf-path" && i+1 < len(args) {
			return args[i+1]
		}
		// -conf-path=value
		if after, ok := strings.CutPrefix(a, "-conf-path="); ok {
			return after
		}
		// -conf-path value
		if a == "-conf-path" && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

// ============================== CLI 模式 ==============================

func runCLI() {
	fmt.Println("[功能]:多线程下载直播流m3u8视屏（支持 TS/fMP4）\n[提醒]:下载失败，请使用 -ht=v2\n[提醒]:fMP4 流需要 ffmpeg 进行合并\n[提醒]:进度条中途下载失败，可重复执行断点续传")
	runtime.GOMAXPROCS(runtime.NumCPU())
	now := time.Now()

	m3u8Url := *urlFlag
	if !strings.HasPrefix(m3u8Url, "http") || m3u8Url == "" {
		flag.Usage()
		return
	}

	pwd, _ := os.Getwd()
	if *spFlag != "" {
		pwd = *spFlag
	}
	outputDir := filepath.Join(pwd, *oFlag)
	if isExist, _ := util.PathExists(outputDir); !isExist {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			fmt.Printf("\n[Failed] 创建目录失败: %v\n", err)
			return
		}
	}

	d := dl.New(dl.Config{
		M3U8URL:    m3u8Url,
		OutputDir:  outputDir,
		MaxWorkers: *nFlag,
		HostType:   *htFlag,
		AutoClear:  *rFlag,
		AutoName:   *oFlag == "movie",
		Cookie:     *cFlag,
		Insecure:   *sFlag != 0,
	})

	if err := d.Parse(); err != nil {
		fmt.Printf("\n[Failed] 解析 m3u8 失败: %v\n", err)
		return
	}
	d.AutoName(pwd)

	fmt.Printf("待下载切片数量: %d", d.SegmentCount())
	if d.IsFmp4() {
		fmt.Print(" (fMP4 格式)")
	}
	if d.IsEncrypted() {
		fmt.Print(" (AES-128 加密)")
	}
	fmt.Println()

	d.DownloadAll()

	if err := d.Verify(); err != nil {
		fmt.Printf("\n[Failed] %v\n", err)
		return
	}

	fmt.Print("正在合并...")
	mv, err := d.Merge()
	if err != nil {
		fmt.Printf("\n[Failed] 合并失败: %v\n", err)
		return
	}

	if d.AutoClear() {
		os.RemoveAll(d.OutputDir())
	}

	util.DrawProgressBar("Merging", 1.0, dl.ProgressWidth, mv)
	fmt.Printf("\n[Success] 下载保存路径：%s | 共耗时: %6.2fs\n", mv, time.Now().Sub(now).Seconds())
}

// ============================== RPC Server 模式 ==============================

func runServer() {
	mgr := task.NewManager(*maxDownload, *nFlag, *sessionFile)

	// 恢复上次未完成的任务
	if *sessionFile != "" {
		if err := mgr.LoadSession(*sessionFile); err != nil {
			log.Printf("[session] 加载失败: %v", err)
		}
	}

	addr := fmt.Sprintf("127.0.0.1:%d", *rpcPort)
	if *rpcListen {
		addr = fmt.Sprintf("0.0.0.0:%d", *rpcPort)
	}

	srv := rpc.NewServer(addr, mgr, *rpcSecret)
	fmt.Printf("[RPC] 服务启动: http://%s/jsonrpc\n", addr)
	if *rpcSecret != "" {
		fmt.Println("[RPC] 鉴权已启用 (token:****)")
	}
	fmt.Println("[RPC] 使用 AriaNg 连接此地址即可管理下载任务")

	// 信号处理：Ctrl+C 或 kill 时优雅关闭
	// 注意：Windows 上 Ctrl+C 发送 os.Interrupt，而非 SIGINT
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// 在 goroutine 中启动服务
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	// 等待信号或启动错误
	select {
	case sig := <-sigCh:
		fmt.Printf("\n[RPC] 收到信号 %v，正在关闭...\n", sig)
		srv.Stop()
		<-errCh // 等待 Serve 返回

		// 最终保存会话（同步，确保不丢数据）
		if *sessionFile != "" {
			if err := mgr.SaveSession(*sessionFile); err != nil {
				log.Printf("[session] 保存失败: %v", err)
			} else {
				log.Println("[session] 会话已保存")
			}
		}
		fmt.Println("[RPC] 服务已停止")
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("[RPC] 服务异常退出: %v", err)
		}
	}
}
