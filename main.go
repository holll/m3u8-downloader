// @author:llychao<lychao_vip@163.com>
// @contributor: Junyi<me@junyi.pw>
// @date:2020-02-18
// @功能:golang m3u8 video Downloader
// @fix:2026-06-30 — fMP4 support, concurrency fixes, proper IV handling, package split
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"m3u8-downloader/dl"
	"m3u8-downloader/util"
)

// ============================== 命令行参数 ==============================

var (
	urlFlag = flag.String("u", "", "m3u8下载地址(http(s)://url/xx/xx/index.m3u8)")
	nFlag   = flag.Int("n", 24, "num:下载线程数(默认24)")
	htFlag  = flag.String("ht", "v1", "hostType:设置getHost的方式(v1: http(s):// + url.Host + filepath.Dir(url.Path); v2: http(s)://+ u.Host")
	oFlag   = flag.String("o", "movie", "movieName:自定义文件名(默认为movie)不带后缀")
	cFlag   = flag.String("c", "", "cookie:自定义请求cookie")
	rFlag   = flag.Bool("r", true, "autoClear:是否自动清除ts文件")
	sFlag   = flag.Int("s", 0, "InsecureSkipVerify:是否允许不安全的请求(默认0)")
	spFlag  = flag.String("sp", "", "savePath:文件保存的绝对路径(默认为当前路径,建议默认值)")
)

func main() {
	Run()
}

// Run 主流程
func Run() {
	fmt.Println("[功能]:多线程下载直播流m3u8视屏（支持 TS/fMP4）\n[提醒]:下载失败，请使用 -ht=v2\n[提醒]:fMP4 流需要 ffmpeg 进行合并\n[提醒]:进度条中途下载失败，可重复执行断点续传")
	runtime.GOMAXPROCS(runtime.NumCPU())
	now := time.Now()

	// 1、解析命令行参数
	flag.Parse()
	m3u8Url := *urlFlag
	if !strings.HasPrefix(m3u8Url, "http") || m3u8Url == "" {
		flag.Usage()
		return
	}

	// 2、创建下载器
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
		Cookie:     *cFlag,
		Insecure:   *sFlag != 0,
	})

	// 3、解析 m3u8
	if err := d.Parse(); err != nil {
		fmt.Printf("\n[Failed] 解析 m3u8 失败: %v\n", err)
		return
	}
	fmt.Printf("待下载切片数量: %d", d.SegmentCount())
	if d.IsFmp4() {
		fmt.Print(" (fMP4 格式)")
	}
	if d.IsEncrypted() {
		fmt.Print(" (AES-128 加密)")
	}
	fmt.Println()

	// 4、下载所有切片
	d.DownloadAll()

	// 5、校验下载结果
	if err := d.Verify(); err != nil {
		fmt.Printf("\n[Failed] %v\n", err)
		return
	}

	// 6、合并切片
	fmt.Println()
	mv, err := d.Merge()
	if err != nil {
		fmt.Printf("\n[Failed] 合并失败: %v\n", err)
		return
	}

	// 7、清理
	if d.AutoClear() {
		os.RemoveAll(d.OutputDir())
	}

	// 8、完成
	util.DrawProgressBar("Merging", 1.0, dl.ProgressWidth, mv)
	fmt.Printf("\n[Success] 下载保存路径：%s | 共耗时: %6.2fs\n", mv, time.Now().Sub(now).Seconds())
}
