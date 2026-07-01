package dl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"m3u8-downloader/crypto"

	"github.com/levigross/grequests"
)

// ============================== 下载 ==============================

// DownloadAll 并发下载所有切片
func (d *Downloader) DownloadAll() {
	d.progress = &ProgressTracker{total: int64(len(d.segments))}

	var wg sync.WaitGroup
	limiter := make(chan struct{}, d.maxWorkers)

	if !d.quiet {
		doneCh := make(chan struct{})
		go func() {
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-doneCh:
					return
				case <-ticker.C:
					d.progress.Draw("Downloading", ProgressWidth)
				}
			}
		}()
		defer func() { close(doneCh) }()
	}

	for i := range d.segments {
		wg.Add(1)
		limiter <- struct{}{}
		go func(seg Segment) {
			defer func() {
				wg.Done()
				<-limiter
			}()
			if err := d.downloadSegment(seg); err != nil {
				d.progress.Fail()
				if !d.quiet {
					Log.Printf("[warn] segment %d failed: %v", seg.Index, err)
				}
			}
			d.progress.Done()
		}(d.segments[i])
	}
	wg.Wait()

	if !d.quiet {
		d.progress.Draw("Downloading", ProgressWidth)
		fmt.Println()
	}
}

func (d *Downloader) downloadSegment(seg Segment) error {
	ext := ".ts"
	if d.isFmp4 {
		ext = ".mp4"
	}
	filePath := filepath.Join(d.outputDir, fmt.Sprintf("%05d%s", seg.Index, ext))

	if info, err := os.Stat(filePath); err == nil && info.Size() > 0 {
		if d.onProgress != nil {
			d.onProgress(int64(seg.Index+1), int64(len(d.segments)))
		}
		return nil
	}

	for attempt := 0; ; attempt++ {
		// 退避延迟：500ms→1s→1.5s→...封顶30s
		delay := time.Duration(attempt+1) * 500 * time.Millisecond
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		if attempt > 0 {
			time.Sleep(delay)
		}

		// 下载 init segment（仅当与切片主体不同文件时才需要）
		if seg.Map != nil && seg.Map.URI != seg.URL {
			initPath := filepath.Join(d.outputDir, fmt.Sprintf("init_%05d.mp4", seg.Index))
			if _, err := os.Stat(initPath); os.IsNotExist(err) {
				if err := d.downloadInitSegment(seg.Map, initPath); err != nil {
					continue
				}
			}
		}

		// 下载切片主体
		err := d.downloadSingle(seg, filePath)
		if err == nil {
			if d.onProgress != nil {
				d.onProgress(int64(seg.Index+1), int64(len(d.segments)))
			}
			return nil
		}
	}
}

func (d *Downloader) downloadSingle(seg Segment, filePath string) error {
	resp, err := grequests.Get(seg.URL, d.reqOpts)
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	data := resp.Bytes()
	if len(data) == 0 {
		return fmt.Errorf("empty response")
	}

	if d.onBytes != nil {
		d.onBytes(int64(len(data)))
	}

	if cl := resp.Header.Get("Content-Length"); cl != "" {
		expected, _ := strconv.Atoi(cl)
		if expected > 0 && len(data) != expected {
			return fmt.Errorf("size mismatch: got %d, expected %d", len(data), expected)
		}
	}

	if seg.Key != nil {
		keyData, err := d.fetchKey(seg.Key)
		if err != nil {
			return fmt.Errorf("fetch key: %w", err)
		}
		iv := seg.Key.IV
		if iv == nil {
			iv = make([]byte, 16)
			binary.BigEndian.PutUint64(iv[8:], uint64(seg.Index))
		}
		data, err = crypto.AesDecrypt(data, keyData, iv)
		if err != nil {
			return fmt.Errorf("decrypt: %w", err)
		}
	}

	if !d.isFmp4 {
		data = stripBeforeSyncByte(data)
	}

	return os.WriteFile(filePath, data, 0644)
}

func (d *Downloader) downloadInitSegment(m *MapInfo, filePath string) error {
	resp, err := grequests.Get(m.URI, d.reqOpts)
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	data := resp.Bytes()
	if m.Limit > 0 && m.Offset+m.Limit <= int64(len(data)) {
		data = data[m.Offset : m.Offset+m.Limit]
	}
	return os.WriteFile(filePath, data, 0644)
}

func (d *Downloader) fetchKey(ki *KeyInfo) ([]byte, error) {
	resp, err := grequests.Get(ki.URI, d.reqOpts)
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, fmt.Errorf("key request HTTP %d", resp.StatusCode)
	}
	keyData := resp.Bytes()
	if len(keyData) != 16 {
		return nil, fmt.Errorf("unexpected key length: %d (expected 16)", len(keyData))
	}
	return keyData, nil
}

func stripBeforeSyncByte(data []byte) []byte {
	idx := bytes.IndexByte(data, 0x47)
	if idx > 0 {
		return data[idx:]
	}
	return data
}
