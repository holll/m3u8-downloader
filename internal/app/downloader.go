package app

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/levigross/grequests"
)

// 分片下载、合并与进度展示。
// downloadTsFile 的策略是：存在且非空即复用；失败自动递归重试。
func downloadTsFile(ts TsInfo, download_dir, key string, retries int, ro *grequests.RequestOptions) bool {
	if retries <= 0 {
		debugf("segment retries exhausted: name=%s url=%s", ts.Name, ts.Url)
		return false
	}
	curr_path_file := fmt.Sprintf("%s/%s", download_dir, ts.Name)
	if isExist, _ := pathExists(curr_path_file); isExist {
		if info, err := os.Stat(curr_path_file); err == nil && info.Size() > 0 {
			return true
		}
		_ = os.Remove(curr_path_file)
	}
	res, err := grequests.Get(ts.Url, ro)
	debugf("segment fetch: name=%s status_ok=%v err=%v", ts.Name, err == nil && res != nil && res.Ok, err)
	if err != nil || !res.Ok {
		if retries > 0 {
			return downloadTsFile(ts, download_dir, key, retries-1, ro)
		} else {
			//logger.Printf("[warn] File :%s", ts.Url)
			return false
		}
	}
	// 校验长度是否合法
	var origData []byte
	origData = res.Bytes()
	contentLen := 0
	contentLenStr := res.Header.Get("Content-Length")
	if contentLenStr != "" {
		contentLen, _ = strconv.Atoi(contentLenStr)
	}
	if len(origData) == 0 || (contentLen > 0 && len(origData) < contentLen) || res.Error != nil {
		//logger.Println("[warn] File: " + ts.Name + "res origData invalid or err：", res.Error)
		return downloadTsFile(ts, download_dir, key, retries-1, ro)
	}
	// 解密出视频 ts 源文件
	if key != "" {
		//解密 ts 文件，算法：aes 128 cbc pack5
		origData, err = AesDecrypt(origData, []byte(key))
		if err != nil {
			return downloadTsFile(ts, download_dir, key, retries-1, ro)
		}
	}
	// https://en.wikipedia.org/wiki/MPEG_transport_stream
	// Some TS files do not start with SyncByte 0x47, they can not be played after merging,
	// Need to remove the bytes before the SyncByte 0x47(71).
	syncByte := uint8(71) //0x47
	bLen := len(origData)
	for j := 0; j < bLen; j++ {
		if origData[j] == syncByte {
			origData = origData[j:]
			break
		}
	}
	if err := os.WriteFile(curr_path_file, origData, 0666); err != nil {
		return false
	}
	return true
}

func downloader(tsList []TsInfo, maxGoroutines int, downloadDir string, key string, ro *grequests.RequestOptions, onProgress ProgressFunc) int {
	retry := 5 //单个ts 下载重试次数
	tsLen := len(tsList)
	if tsLen == 0 {
		return 0
	}
	if maxGoroutines <= 0 {
		maxGoroutines = 1
	}
	var downloadCount int64
	pending := tsList
	maxRounds := 3
	for round := 1; round <= maxRounds && len(pending) > 0; round++ {
		failed := make([]TsInfo, 0)
		var mu sync.Mutex
		var wg sync.WaitGroup
		concurrency := maxGoroutines / round
		if concurrency <= 0 {
			concurrency = 1
		}
		limiter := make(chan struct{}, concurrency)
		for _, ts := range pending {
			wg.Add(1)
			limiter <- struct{}{}
			go func(ts TsInfo) {
				defer func() {
					wg.Done()
					<-limiter
				}()
				if downloadTsFile(ts, downloadDir, key, retry, ro) {
					count := atomic.AddInt64(&downloadCount, 1)
					DrawProgressBar("Downloading", float32(count)/float32(tsLen), PROGRESS_WIDTH, ts.Name)
					if onProgress != nil {
						onProgress(int(count), tsLen)
					}
					return
				}
				mu.Lock()
				failed = append(failed, ts)
				mu.Unlock()
			}(ts)
		}
		wg.Wait()
		if len(failed) > 0 && round < maxRounds {
			debugf("round=%d failed=%d concurrency=%d", round, len(failed), concurrency)
			fmt.Printf("\n[warn] 第%d轮失败分片: %d，准备重试...\n", round, len(failed))
			time.Sleep(time.Duration(round) * 500 * time.Millisecond)
		}
		pending = failed
	}
	return int(downloadCount)
}

// mergeTs 按 tsList 原始顺序拼接，避免文件系统遍历顺序导致的视频错乱。
func mergeTs(downloadDir, outputPath string, tsList []TsInfo) (string, error) {
	outMv, err := os.Create(outputPath)
	if err != nil {
		return "", err
	}
	defer outMv.Close()
	writer := bufio.NewWriter(outMv)
	for _, ts := range tsList {
		path := filepath.Join(downloadDir, ts.Name)
		bytes, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		if _, err = writer.Write(bytes); err != nil {
			return "", err
		}
	}
	if err = writer.Flush(); err != nil {
		return "", err
	}
	return outputPath, nil
}

func DrawProgressBar(prefix string, proportion float32, width int, suffix ...string) {
	pos := int(proportion * float32(width))
	s := fmt.Sprintf("[%s] %s%*s %6.2f%% \t%s",
		prefix, strings.Repeat("■", pos), width-pos, "", proportion*100, strings.Join(suffix, ""))
	fmt.Print("\r" + s)
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
