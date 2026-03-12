package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Run 是程序主流程入口：解析参数后进入 CLI 或 API 模式。
func Run() {
	msgTpl := "[功能]:多线程下载直播流m3u8视屏\n[提醒]:下载失败，请使用 -ht=v2 \n[提醒]:下载失败，m3u8 地址可能存在嵌套\n[提醒]:进度条中途下载失败，可重复执行"
	fmt.Println(msgTpl)
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	if *apiFlag != "" {
		if *rpcSecretFlag == "" {
			fmt.Println("[Failed] API模式下必须设置 -rpc-secret")
			return
		}
		runAPIServer(*apiFlag, *jFlag, *rpcSecretFlag)
		return
	}
	job := DownloadJob{
		M3U8URL:       *urlFlag,
		MaxGoroutines: *nFlag,
		HostType:      *htFlag,
		MovieName:     *oFlag,
		OutputName:    *oFlag + ".mp4",
		AutoClear:     *rFlag,
		Cookie:        *cFlag,
		Insecure:      *sFlag,
		SavePath:      *spFlag,
	}
	mv, err := runDownload(job, nil)
	if err != nil {
		fmt.Printf("\n[Failed] %v\n", err)
		return
	}
	DrawProgressBar("Merging", float32(1), PROGRESS_WIDTH, mv)
}

// runDownload 负责执行单任务下载。
func runDownload(job DownloadJob, onProgress ProgressFunc) (string, error) {
	now := time.Now()
	if !strings.HasPrefix(job.M3U8URL, "http") || job.M3U8URL == "" {
		return "", fmt.Errorf("invalid m3u8 url")
	}
	if job.MaxGoroutines <= 0 {
		job.MaxGoroutines = 1
	}
	if job.HostType == "" {
		job.HostType = "v1"
	}
	if job.MovieName == "" && job.OutputName != "" {
		job.MovieName = strings.TrimSuffix(job.OutputName, filepath.Ext(job.OutputName))
	}
	if job.MovieName == "" {
		job.MovieName = "movie"
	}
	if job.OutputName == "" {
		job.OutputName = job.MovieName + ".mp4"
	}
	pwd, _ := os.Getwd()
	if job.SavePath != "" {
		pwd = job.SavePath
	}
	outputPath := filepath.Join(pwd, job.OutputName)
	tmpDir := buildPartsDir(pwd, job.M3U8URL)
	if isExist, _ := pathExists(tmpDir); !isExist {
		_ = os.MkdirAll(tmpDir, os.ModePerm)
	}

	ro := ro
	ro.Headers = cloneHeaders(ro.Headers)
	ro.Headers["Referer"] = getHost(job.M3U8URL, "v2")
	if job.Insecure != 0 {
		ro.InsecureSkipVerify = true
	}
	if job.Cookie != "" {
		ro.Headers["Cookie"] = job.Cookie
	}

	m3u8Host := getHost(job.M3U8URL, job.HostType)
	m3u8Body := getM3u8Body(job.M3U8URL, &ro)
	tsKey := getM3u8Key(m3u8Host, m3u8Body, &ro)
	if tsKey != "" {
		fmt.Printf("待解密 ts 文件 key : %s \n", tsKey)
	}
	tsList := getTsList(m3u8Host, m3u8Body)
	if len(tsList) == 0 {
		return "", fmt.Errorf("m3u8中未解析到ts分片")
	}
	fmt.Println("待下载 ts 文件数量:", len(tsList))
	if onProgress != nil {
		onProgress(0, len(tsList))
	}

	tsRO := ro
	tsRO.RequestTimeout = TS_TIMEOUT
	okCount := downloader(tsList, job.MaxGoroutines, tmpDir, tsKey, &tsRO, onProgress)
	if okCount != len(tsList) {
		return "", fmt.Errorf("ts下载不完整: %d/%d", okCount, len(tsList))
	}
	mv, err := mergeTs(tmpDir, outputPath, tsList)
	if err != nil {
		return "", err
	}
	if job.AutoClear {
		_ = os.RemoveAll(tmpDir)
	}
	fmt.Printf("\n[Success] 下载保存路径：%s | 共耗时: %6.2fs\n", mv, time.Now().Sub(now).Seconds())
	return mv, nil
}

// buildPartsDir 根据下载地址生成稳定的临时分片目录，避免任务冲突。
func buildPartsDir(basePath, m3u8URL string) string {
	sum := sha1.Sum([]byte(strings.TrimSpace(m3u8URL)))
	return filepath.Join(basePath, fmt.Sprintf("%x.parts", sum[:8]))
}

func cloneHeaders(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
