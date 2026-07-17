// @author:llychao<lychao_vip@163.com>
// @contributor: Junyi<me@junyi.pw>
// @date:2020-02-18
// @功能:golang m3u8 video Downloader (CLI + aria2 RPC)
// @fix:2026-06-30 — fMP4 support, concurrency fixes, proper IV handling, aria2 RPC server
package main

import (
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

	"github.com/urfave/cli/v2"

	"m3u8-downloader/config"
	"m3u8-downloader/dl"
	"m3u8-downloader/rpc"
	"m3u8-downloader/sprite"
	"m3u8-downloader/task"
	"m3u8-downloader/util"
)

func main() {
	app := &cli.App{
		Name:  "m3u8-downloader",
		Usage: "多线程下载直播流 m3u8 视频（支持 TS/fMP4、AES-128 解密、aria2 RPC）",
		Flags: globalFlags(),
		Action: func(c *cli.Context) error {
			// 配置文件用于填写 Server 模式参数（如 rpc-listen-port 等）
			loadConfig(c.String("conf-path"))

			if c.Int("rpc-listen-port") != 0 {
				runServer(c)
			} else {
				runCLI(c)
			}
			return nil
		},
		Commands: []*cli.Command{
			spriteCommand(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

// ============================== 全局参数 ==============================

func globalFlags() []cli.Flag {
	return []cli.Flag{
		// CLI 模式
		&cli.StringFlag{Name: "u", Usage: "m3u8 下载地址", EnvVars: []string{"M3U8_URL"}},
		&cli.IntFlag{Name: "n", Value: 3, Usage: "下载线程数"},
		&cli.StringFlag{Name: "ht", Value: "v1", Usage: "host 拼接策略: v1/v2"},
		&cli.StringFlag{Name: "o", Value: "movie", Usage: "输出文件名（不含扩展名）"},
		&cli.StringFlag{Name: "c", Usage: "自定义请求 Cookie"},
		&cli.BoolFlag{Name: "r", Value: true, Usage: "完成后自动清除临时 ts 文件"},
		&cli.IntFlag{Name: "s", Value: 0, Usage: "跳过 HTTPS 证书校验（1=跳过）"},
		&cli.StringFlag{Name: "sp", Usage: "文件保存路径（绝对路径）"},
		&cli.IntFlag{Name: "max-retry", Value: 5, Usage: "单分片最大重试次数"},
		// RPC Server 模式
		&cli.IntFlag{Name: "rpc-listen-port", Value: 0, Usage: "RPC 监听端口（0=CLI模式）"},
		&cli.StringFlag{Name: "rpc-secret", Usage: "RPC 鉴权密钥"},
		&cli.BoolFlag{Name: "rpc-listen-all", Usage: "监听所有网卡（默认仅 127.0.0.1）"},
		&cli.IntFlag{Name: "max-concurrent-downloads", Value: 1, Usage: "最大同时下载任务数"},
		&cli.StringFlag{Name: "session-file", Value: "m3u8.session", Usage: "会话文件路径"},
		&cli.StringFlag{Name: "conf-path", Usage: "配置文件路径（key=value 格式，用于配置 Server 模式参数）"},
	}
}

// ============================== sprite 子命令 ==============================

func spriteCommand() *cli.Command {
	return &cli.Command{
		Name:  "sprite",
		Usage: "从 m3u8 生成雪碧图（缩略图网格）",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "u", Required: true, Usage: "m3u8 URL"},
			&cli.IntFlag{Name: "cols", Value: 5, Usage: "雪碧图列数"},
			&cli.IntFlag{Name: "rows", Value: 4, Usage: "雪碧图行数"},
			&cli.IntFlag{Name: "w", Value: 480, Usage: "缩略图宽度 px"},
			&cli.IntFlag{Name: "height", Value: 270, Usage: "缩略图高度 px"},
			&cli.StringFlag{Name: "o", Value: "sprite", Usage: "输出文件名前缀"},
			&cli.StringFlag{Name: "sp", Usage: "输出目录（默认当前目录）"},
			&cli.IntFlag{Name: "n", Value: 3, Usage: "下载线程数"},
			&cli.StringFlag{Name: "ht", Value: "v1", Usage: "hostType: v1/v2"},
			&cli.StringFlag{Name: "c", Usage: "自定义请求 Cookie"},
			&cli.IntFlag{Name: "s", Value: 0, Usage: "跳过 TLS 验证（1=跳过）"},
			&cli.IntFlag{Name: "max-retry", Value: 5, Usage: "单分片最大重试次数"},
		},
		Action: func(c *cli.Context) error {
			savePath := c.String("sp")
			if savePath == "" {
				savePath, _ = os.Getwd()
			}
			o := c.String("o")
			return sprite.Run(sprite.Config{
				URL:        c.String("u"),
				OutPrefix:  o,
				AutoName:   o == "sprite",
				SavePath:   savePath,
				HostType:   c.String("ht"),
				Cookie:     c.String("c"),
				Insecure:   c.Int("s") != 0,
				Cols:       c.Int("cols"),
				Rows:       c.Int("rows"),
				ThumbW:     c.Int("w"),
				ThumbH:     c.Int("height"),
				MaxWorkers: c.Int("n"),
				MaxRetry:   c.Int("max-retry"),
			})
		},
	}
}

// ============================== 配置文件加载 ==============================

func loadConfig(cfgPath string) {
	if cfgPath == "" {
		return
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Printf("[config] 加载失败: %v", err)
		return
	}
	// 将配置文件中的值设置为环境变量，urfave/cli 会通过 EnvVars 读取
	// 对于没有 EnvVars 的 flag，通过 os.Args 注入不现实，保持兼容即可
	for k, v := range cfg {
		os.Setenv("M3U8_"+strings.ToUpper(strings.ReplaceAll(k, "-", "_")), v)
	}
	log.Printf("[config] 已从 %s 加载 %d 条配置", cfgPath, len(cfg))
}

// ============================== CLI 模式 ==============================

func runCLI(c *cli.Context) {
	fmt.Println("[功能]:多线程下载直播流m3u8视屏（支持 TS/fMP4）\n[提醒]:下载失败，请使用 -ht=v2\n[提醒]:fMP4 流需要 ffmpeg 进行合并\n[提醒]:进度条中途下载失败，可重复执行断点续传")
	runtime.GOMAXPROCS(runtime.NumCPU())
	now := time.Now()

	m3u8Url := c.String("u")
	if !strings.HasPrefix(m3u8Url, "http") || m3u8Url == "" {
		cli.ShowAppHelp(c)
		return
	}

	pwd, _ := os.Getwd()
	if c.String("sp") != "" {
		pwd = c.String("sp")
	}
	oName := c.String("o")
	outputDir := filepath.Join(pwd, oName)
	if isExist, _ := util.PathExists(outputDir); !isExist {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			fmt.Printf("\n[Failed] 创建目录失败: %v\n", err)
			return
		}
	}

	d := dl.New(dl.Config{
		M3U8URL:    m3u8Url,
		OutputDir:  outputDir,
		MaxWorkers: c.Int("n"),
		HostType:   c.String("ht"),
		AutoClear:  c.Bool("r"),
		AutoName:   oName == "movie",
		Cookie:     c.String("c"),
		Insecure:   c.Int("s") != 0,
		MaxRetry:   c.Int("max-retry"),
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
	fmt.Printf("\n[Success] 下载保存路径：%s | 共耗时: %6.2fs\n", mv, time.Since(now).Seconds())
}

// ============================== RPC Server 模式 ==============================

func runServer(c *cli.Context) {
	dir := c.String("sp")
	if dir == "" {
		dir = "."
	}

	sessionPath := c.String("session-file")
	mgr := task.NewManager(c.Int("max-concurrent-downloads"), c.Int("n"), dir, sessionPath)

	if sessionPath != "" {
		if err := mgr.LoadSession(sessionPath); err != nil {
			log.Printf("[session] 加载失败: %v", err)
		}
	}

	shutdownCh := make(chan struct{})
	mgr.SetShutdownCallback(func() {
		select {
		case <-shutdownCh:
		default:
			close(shutdownCh)
		}
	})

	port := c.Int("rpc-listen-port")
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if c.Bool("rpc-listen-all") {
		addr = fmt.Sprintf("0.0.0.0:%d", port)
	}

	secret := c.String("rpc-secret")
	srv := rpc.NewServer(addr, mgr, secret)
	fmt.Printf("[RPC] 服务启动: http://%s/jsonrpc\n", addr)
	if secret != "" {
		fmt.Println("[RPC] 鉴权已启用 (token:****)")
	}
	fmt.Println("[RPC] 使用 AriaNg 连接此地址即可管理下载任务")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	gracefulStop := func(reason string) {
		fmt.Printf("\n[RPC] %s，正在关闭...\n", reason)
		srv.Stop()
		<-errCh

		if sessionPath != "" {
			if err := mgr.SaveSession(sessionPath); err != nil {
				log.Printf("[session] 保存失败: %v", err)
			} else {
				log.Println("[session] 会话已保存")
			}
		}
		fmt.Println("[RPC] 服务已停止")
	}

	select {
	case sig := <-sigCh:
		gracefulStop(fmt.Sprintf("收到信号 %v", sig))
	case <-shutdownCh:
		gracefulStop("收到 shutdown 请求")
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("[RPC] 服务异常退出: %v", err)
		}
	}
}
