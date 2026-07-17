// Package sprite 从 m3u8 视频流生成雪碧图（缩略图网格）。
package sprite

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"m3u8-downloader/dl"
)

// Config 雪碧图生成配置
type Config struct {
	URL       string // m3u8 地址（必填）
	OutPrefix string // 输出文件名前缀（默认 "sprite"）
	AutoName  bool   // 是否从 ProgramDateTime 自动命名（用户未指定 -o 时为 true）
	SavePath  string // 输出目录（默认 cwd）
	HostType  string // v1 / v2
	Cookie    string
	Insecure  bool

	Cols       int // 网格列数，默认 5
	Rows       int // 网格行数，默认 4
	ThumbW     int // 缩略图宽度 px，默认 160
	ThumbH     int // 缩略图高度 px，默认 90
	MaxWorkers int
	MaxRetry   int
}

// samplePoint 单个采样点：分片索引 + 片内偏移秒数 + 对应的全局时间戳
type samplePoint struct {
	segIdx    int
	offsetSec float64
	globalSec float64
}

// Run 执行雪碧图生成的完整流程
func Run(cfg Config) error {
	// 1. 检查 ffmpeg
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return fmt.Errorf("ffmpeg 未安装，请安装后重试")
	}

	// 2. 创建临时分片目录
	segsDir := filepath.Join(cfg.SavePath, cfg.OutPrefix+"_segs")
	if err := os.MkdirAll(segsDir, 0755); err != nil {
		return fmt.Errorf("创建临时目录失败: %w", err)
	}
	defer os.RemoveAll(segsDir)

	// 3. 解析 m3u8
	fmt.Println("[sprite] 解析 m3u8...")
	d := dl.New(dl.Config{
		M3U8URL:    cfg.URL,
		OutputDir:  segsDir,
		MaxWorkers: cfg.MaxWorkers,
		HostType:   cfg.HostType,
		Cookie:     cfg.Cookie,
		Insecure:   cfg.Insecure,
		MaxRetry:   cfg.MaxRetry,
		Quiet:      false,
	})
	if err := d.Parse(); err != nil {
		return fmt.Errorf("解析 m3u8 失败: %w", err)
	}

	segs := d.Segments()
	if len(segs) == 0 {
		return fmt.Errorf("未解析到任何分片")
	}

	// 自动命名：从 ProgramDateTime 生成文件名前缀
	if cfg.AutoName {
		for _, seg := range segs {
			if !seg.ProgramDateTime.IsZero() {
				cfg.OutPrefix = seg.ProgramDateTime.Format("20060102_150405")
				fmt.Printf("[sprite] 自动识别文件名: %s.jpg\n", cfg.OutPrefix)
				break
			}
		}
	}

	// 4. 计算累计时间戳 & 总时长
	cumulative := make([]float64, len(segs)+1)
	for i, s := range segs {
		cumulative[i+1] = cumulative[i] + s.Duration
	}
	totalDuration := cumulative[len(segs)]

	// 5. 目标帧数 = rows * cols，间隔由程序自动计算
	cols := cfg.Cols
	rows := cfg.Rows
	targetN := cols * rows
	fmt.Printf("[sprite] 网格 %d 列 x %d 行 = %d 帧，视频时长 %.1fs，采样间隔 %.1fs\n",
		cols, rows, targetN, totalDuration, totalDuration/float64(targetN))

	// 6. 均匀选取采样点
	samples := buildSamplePoints(segs, cumulative, totalDuration, targetN)
	if len(samples) == 0 {
		return fmt.Errorf("未能确定采样点（视频时长为 0 或分片无效）")
	}

	// 6. 只下载采样点涉及的分片
	segIndices := make([]int, 0, len(samples))
	seen := make(map[int]struct{}, len(samples))
	for _, sp := range samples {
		idx := segs[sp.segIdx].Index
		if _, dup := seen[idx]; !dup {
			seen[idx] = struct{}{}
			segIndices = append(segIndices, idx)
		}
	}
	fmt.Printf("[sprite] 下载采样分片（%d / %d 个）...\n", len(segIndices), len(segs))
	d.DownloadSelected(segIndices)

	// 7. 提取每个采样点的缩略帧
	fmt.Printf("[sprite] 提取 %d 帧缩略图...\n", len(samples))
	isFmp4 := d.IsFmp4()
	framesDir := filepath.Join(cfg.SavePath, cfg.OutPrefix+"_frames")
	if err := os.MkdirAll(framesDir, 0755); err != nil {
		return fmt.Errorf("创建帧目录失败: %w", err)
	}
	defer os.RemoveAll(framesDir)

	var frameFiles []string
	for i, sp := range samples {
		seg := segs[sp.segIdx]
		var segFile string
		if isFmp4 {
			segFile = filepath.Join(segsDir, fmt.Sprintf("%05d.mp4", seg.Index))
		} else {
			segFile = filepath.Join(segsDir, fmt.Sprintf("%05d.ts", seg.Index))
		}
		frameFile := filepath.Join(framesDir, fmt.Sprintf("frame_%04d.jpg", i))

		var err error
		if isFmp4 {
			initFile := filepath.Join(segsDir, fmt.Sprintf("init_%05d.mp4", seg.Index))
			if _, statErr := os.Stat(initFile); statErr == nil {
				err = extractFrameFmp4(initFile, segFile, frameFile, sp.offsetSec, cfg.ThumbW, cfg.ThumbH, framesDir, i)
			} else {
				// init 段不存在，分片自包含，直接提取
				err = extractFrame(segFile, frameFile, sp.offsetSec, cfg.ThumbW, cfg.ThumbH)
			}
		} else {
			err = extractFrame(segFile, frameFile, sp.offsetSec, cfg.ThumbW, cfg.ThumbH)
		}
		if err != nil {
			fmt.Printf("[sprite] warn: 帧 %d 提取失败（%s）: %v，跳过\n", i, filepath.Base(segFile), err)
			continue
		}
		frameFiles = append(frameFiles, frameFile)
	}

	if len(frameFiles) == 0 {
		return fmt.Errorf("未能提取到任何缩略帧")
	}

	// 实际帧数不足时修正 rows
	actualRows := (len(frameFiles) + cols - 1) / cols

	// 8. 拼接雪碧图
	outImage := filepath.Join(cfg.SavePath, cfg.OutPrefix+".jpg")
	fmt.Printf("[sprite] 拼接雪碧图 %dx%d（%d 帧）...\n", cols, actualRows, len(frameFiles))
	if err := buildSprite(frameFiles, cols, actualRows, outImage, framesDir); err != nil {
		return fmt.Errorf("生成雪碧图失败: %w", err)
	}

	fmt.Printf("[sprite] 完成: %s\n", outImage)
	return nil
}

// buildSamplePoints 均匀计算 targetN 个采样点
func buildSamplePoints(segs []dl.Segment, cumulative []float64, totalDur float64, targetN int) []samplePoint {
	if totalDur <= 0 || targetN <= 0 {
		return nil
	}
	var points []samplePoint
	for i := 0; i < targetN; i++ {
		t := float64(i) * totalDur / float64(targetN)
		// 找到覆盖时间点 t 的分片
		segIdx := findSegment(cumulative, t)
		if segIdx < 0 || segIdx >= len(segs) {
			continue
		}
		seg := segs[segIdx]
		if seg.Duration <= 0 {
			continue
		}
		offset := t - cumulative[segIdx]
		// 钳制到 [0, duration*0.95]，避免越界
		if offset < 0 {
			offset = 0
		}
		if offset > seg.Duration*0.95 {
			offset = seg.Duration * 0.95
		}
		points = append(points, samplePoint{
			segIdx:    segIdx,
			offsetSec: offset,
			globalSec: t,
		})
	}
	return points
}

// findSegment 二分查找覆盖时间点 t 的分片索引（cumulative[i] <= t < cumulative[i+1]）
func findSegment(cumulative []float64, t float64) int {
	lo, hi := 0, len(cumulative)-2
	for lo <= hi {
		mid := (lo + hi) / 2
		if cumulative[mid] <= t && t < cumulative[mid+1] {
			return mid
		} else if t < cumulative[mid] {
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	// t == totalDuration 时取最后一个分片
	return len(cumulative) - 2
}

// scaleFilter 构建 scale+pad 滤镜字符串（保持比例，填充黑边到 WxH）
func scaleFilter(w, h int) string {
	return fmt.Sprintf(
		"scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
		w, h, w, h,
	)
}

// extractFrame 从 TS 分片提取单帧 JPEG
func extractFrame(segFile, outJpeg string, offsetSec float64, w, h int) error {
	cmd := exec.Command("ffmpeg",
		"-y",
		"-ss", fmt.Sprintf("%.3f", offsetSec),
		"-i", segFile,
		"-vf", scaleFilter(w, h),
		"-vframes", "1",
		"-q:v", "2",
		outJpeg,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w\n%s", err, stderr.String())
	}
	return nil
}

// extractFrameFmp4 从 fMP4 分片提取单帧（需要 init 段解码）
func extractFrameFmp4(initFile, segFile, outJpeg string, offsetSec float64, w, h int, tmpDir string, idx int) error {
	// 写一个临时 concat list，将 init + 分片拼给 ffmpeg 解码
	listPath := filepath.Join(tmpDir, fmt.Sprintf("init_list_%04d.txt", idx))
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "file '%s'\n", filepath.ToSlash(initFile))
	fmt.Fprintf(&buf, "file '%s'\n", filepath.ToSlash(segFile))
	if err := os.WriteFile(listPath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("写 concat list: %w", err)
	}
	defer os.Remove(listPath)

	cmd := exec.Command("ffmpeg",
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listPath,
		"-ss", fmt.Sprintf("%.3f", offsetSec),
		"-vf", scaleFilter(w, h),
		"-vframes", "1",
		"-q:v", "2",
		outJpeg,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w\n%s", err, stderr.String())
	}
	return nil
}

// buildSprite 将帧文件列表拼接为雪碧图
func buildSprite(frameFiles []string, cols, rows int, outPath, tmpDir string) error {
	listPath := filepath.Join(tmpDir, "concat_frames.txt")
	var buf bytes.Buffer
	for _, f := range frameFiles {
		fmt.Fprintf(&buf, "file '%s'\n", filepath.ToSlash(f))
	}
	if err := os.WriteFile(listPath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("写帧列表: %w", err)
	}
	defer os.Remove(listPath)

	cmd := exec.Command("ffmpeg",
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listPath,
		"-vf", fmt.Sprintf("tile=%dx%d", cols, rows),
		"-frames:v", "1",
		"-update", "1",
		"-q:v", "2",
		outPath,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffmpeg tile: %w\n%s", err, stderr.String())
	}
	return nil
}

// writeVTT 和 formatVTTTime 已移除（不再生成 WebVTT）
