package dl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"m3u8-downloader/crypto"

	"github.com/levigross/grequests"
)

// ============================== 下载 ==============================

// DownloadAll 并发下载所有切片
func (d *Downloader) DownloadAll() {
	d.downloadSegments(d.segments)
}

// setSegmentError 记录首个分片失败原因（仅首次生效，并发安全）。
func (d *Downloader) setSegmentError(err error) {
	d.segErrOnce.Do(func() { d.segErr = err })
}

// SegmentError 返回本次下载中首个分片失败原因，全部分片成功时为 nil。
//
// DownloadAll 不返回错误以保持既有调用签名；分片失败若被忽略，Merge 只会
// 把现存分片拼接起来并报成功，产出缺少片段的视频，因此失败原因必须外传，
// 由上层决定是否终止任务。
func (d *Downloader) SegmentError() error { return d.segErr }

// notifySegmentDone 通知外部该切片已就绪（新下载或断点续传时已存在）。
func (d *Downloader) notifySegmentDone(seg Segment) {
	if d.onSegmentDone != nil {
		d.onSegmentDone(seg.Index)
	}
	if d.onProgress != nil {
		d.onProgress(int64(seg.Index+1), int64(len(d.segments)))
	}
}

// DownloadSelected 并发下载指定索引的切片（索引为 Segment.Index 字段值）
func (d *Downloader) DownloadSelected(indices []int) {
	set := make(map[int]struct{}, len(indices))
	for _, idx := range indices {
		set[idx] = struct{}{}
	}
	var selected []Segment
	for _, seg := range d.segments {
		if _, ok := set[seg.Index]; ok {
			selected = append(selected, seg)
		}
	}
	d.downloadSegments(selected)
}

func (d *Downloader) downloadSegments(segs []Segment) {
	d.progress = &ProgressTracker{total: int64(len(segs))}
	// 每次下载重置失败记录（本方法可能被 DownloadAll / DownloadSelected 复用同一实例）
	d.segErrOnce = sync.Once{}
	d.segErr = nil

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

	for i := range segs {
		wg.Add(1)
		limiter <- struct{}{}
		go func(seg Segment) {
			defer func() {
				wg.Done()
				<-limiter
			}()
			atomic.AddInt64(&d.inflight, 1)
			defer atomic.AddInt64(&d.inflight, -1)

			if err := d.downloadSegment(seg); err != nil {
				d.progress.Fail()
				d.setSegmentError(err)
				if !d.quiet {
					Log.Printf("[warn] segment %d failed: %v", seg.Index, err)
				}
			}
			d.progress.Done()
		}(segs[i])
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
		// fMP4: 已存在文件需校验完整性，损坏则删除并重新下载
		if d.isFmp4 {
			if err := validateFmp4Segment(filePath); err != nil {
				Log.Printf("[warn] segment %d: existing file corrupted (%v), re-downloading", seg.Index, err)
				os.Remove(filePath)
				// 继续走下载流程（不 return）
			} else {
				d.notifySegmentDone(seg)
				return nil
			}
		} else {
			// TS: 简单跳过
			d.notifySegmentDone(seg)
			return nil
		}
	}

	// 保留最后一次失败原因，供上层分类为 aria2 errorCode（HTTP 404 / 网络问题等）
	var lastErr error

	for attempt := 0; attempt <= d.maxRetry; attempt++ {
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
					lastErr = fmt.Errorf("init segment: %w", err)
					continue
				}
			}
		}

		// 下载切片主体
		err := d.downloadSingle(seg, filePath)
		if err == nil {
			d.notifySegmentDone(seg)
			return nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return fmt.Errorf("segment %d failed after %d retries: %w", seg.Index, d.maxRetry+1, lastErr)
	}
	return fmt.Errorf("segment %d failed after %d retries", seg.Index, d.maxRetry+1)
}

func (d *Downloader) downloadSingle(seg Segment, filePath string) error {
	resp, err := grequests.Get(seg.URL, d.reqOpts)
	if err != nil {
		return err
	}
	if !resp.Ok {
		return newHTTPStatusError(resp.StatusCode, seg.URL)
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

	// fMP4: 写盘前直接校验内存中的 ISOBMFF box 链，避免截断/损坏数据落盘。
	// 校验失败时无需删文件，也省去写盘后二次 open 读校验的开销（分片量大时显著）。
	if d.isFmp4 {
		if err := validateFmp4Data(data); err != nil {
			return fmt.Errorf("segment validation failed: %w", err)
		}
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return err
	}

	return nil
}

func (d *Downloader) downloadInitSegment(m *MapInfo, filePath string) error {
	resp, err := grequests.Get(m.URI, d.reqOpts)
	if err != nil {
		return err
	}
	if !resp.Ok {
		return newHTTPStatusError(resp.StatusCode, m.URI)
	}
	data := resp.Bytes()
	if m.Limit > 0 && m.Offset+m.Limit <= int64(len(data)) {
		data = data[m.Offset : m.Offset+m.Limit]
	}
	return os.WriteFile(filePath, data, 0644)
}

func (d *Downloader) fetchKey(ki *KeyInfo) ([]byte, error) {
	d.keyCacheMu.Lock()
	if cached, ok := d.keyCache[ki.URI]; ok {
		d.keyCacheMu.Unlock()
		return cached, nil
	}
	d.keyCacheMu.Unlock()

	resp, err := grequests.Get(ki.URI, d.reqOpts)
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, fmt.Errorf("key request: %w", newHTTPStatusError(resp.StatusCode, ki.URI))
	}
	keyData := resp.Bytes()
	if len(keyData) != 16 {
		return nil, fmt.Errorf("unexpected key length: %d (expected 16)", len(keyData))
	}

	d.keyCacheMu.Lock()
	d.keyCache[ki.URI] = keyData
	d.keyCacheMu.Unlock()

	return keyData, nil
}

func stripBeforeSyncByte(data []byte) []byte {
	idx := bytes.IndexByte(data, 0x47)
	if idx > 0 {
		return data[idx:]
	}
	return data
}

// ============================== 分片校验 ==============================

// validateFmp4Data 校验 fMP4 分片内存数据的 ISOBMFF box 链完整性。
// 遍历所有 box，检查每个 box 是否在数据范围内，用于检测截断或损坏。
// 返回 nil 表示 box 链完整、所有 box 均不超出数据边界。
func validateFmp4Data(data []byte) error {
	fileSize := int64(len(data))
	if fileSize < 8 {
		return fmt.Errorf("file too small: %d bytes", fileSize)
	}

	// 合法的 ISOBMFF box 类型白名单
	validTypes := map[string]bool{
		"ftyp": true, "styp": true, "moof": true, "mdat": true,
		"moov": true, "free": true, "skip": true, "sidx": true,
		"ssix": true, "prft": true, "emsg": true, "uuid": true,
	}

	const maxBoxes = 10000 // 防死循环（正常 fMP4 分片只有 3~10 个 box）
	offset := int64(0)

	for i := 0; i < maxBoxes; i++ {
		// 刚好到达末尾 → box 链完整
		if offset >= fileSize {
			return nil
		}
		// 剩余不足 8 字节 → 数据中间截断
		if offset+8 > fileSize {
			return fmt.Errorf("box #%d @ offset %d: incomplete header, only %d bytes remain (truncated mid-box)",
				i, offset, fileSize-offset)
		}

		size := binary.BigEndian.Uint32(data[offset : offset+4])
		boxType := string(data[offset+4 : offset+8])

		// 第一个 box 类型必须合法
		if i == 0 && !validTypes[boxType] {
			return fmt.Errorf("unexpected box type %q (corrupted or encrypted)", boxType)
		}

		// size == 0: box 延伸到 EOF（合法终止）
		if size == 0 {
			return nil
		}

		// size == 1: 扩展 64-bit size
		effectiveSize := int64(size)
		if size == 1 {
			if offset+16 > fileSize {
				return fmt.Errorf("box #%d @ offset %d: extended-size header truncated", i, offset)
			}
			effectiveSize = int64(binary.BigEndian.Uint64(data[offset+8 : offset+16]))
		}

		// 检查 box 是否超出数据范围
		nextOffset := offset + effectiveSize
		if nextOffset > fileSize {
			return fmt.Errorf("box #%d @ offset %d type=%q: declares %d bytes but only %d remain (truncated)",
				i, offset, boxType, effectiveSize, fileSize-offset)
		}

		offset = nextOffset
	}

	return fmt.Errorf("too many boxes (>%d), likely corrupted", maxBoxes)
}

// validateFmp4Segment 读取磁盘上的 fMP4 分片并校验其 box 链完整性。
func validateFmp4Segment(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	return validateFmp4Data(data)
}
