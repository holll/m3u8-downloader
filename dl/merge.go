package dl

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"m3u8-downloader/util"
)

// ============================== 校验 ==============================

// Verify 验证下载结果是否完整
func (d *Downloader) Verify() error {
	if len(d.segments) == 0 {
		return fmt.Errorf("未解析到任何切片")
	}
	ext := ".ts"
	if d.isFmp4 {
		ext = ".mp4"
	}
	firstFile := filepath.Join(d.outputDir, fmt.Sprintf("%05d%s", 0, ext))
	if exists, _ := util.PathExists(firstFile); !exists {
		return fmt.Errorf("下载似乎未完成：%s 不存在，请检查 URL 有效性", firstFile)
	}
	files, _ := filepath.Glob(filepath.Join(d.outputDir, "*"+ext))
	if len(files) == 0 {
		return fmt.Errorf("下载目录中无切片文件，请检查 URL 有效性")
	}
	return nil
}

// ============================== 合并 ==============================

// Merge 合并切片为最终视频文件，返回输出文件路径
func (d *Downloader) Merge() (string, error) {
	if d.isFmp4 {
		return d.mergeFmp4()
	}
	return d.mergeTS()
}

func (d *Downloader) mergeTS() (string, error) {
	files, err := filepath.Glob(filepath.Join(d.outputDir, "*.ts"))
	if err != nil {
		return "", fmt.Errorf("glob ts files: %w", err)
	}
	if len(files) == 0 {
		return "", fmt.Errorf("no .ts files found in %s", d.outputDir)
	}
	sort.Strings(files)

	out, err := os.Create(d.outputFile)
	if err != nil {
		return "", fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	writer := bufio.NewWriter(out)
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return "", fmt.Errorf("read %s: %w", f, err)
		}
		if _, err := writer.Write(data); err != nil {
			return "", fmt.Errorf("write: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return "", err
	}
	return d.outputFile, nil
}

func (d *Downloader) mergeFmp4() (string, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return "", fmt.Errorf("ffmpeg 未安装，无法合并 fMP4 流；请安装 ffmpeg 后重试")
	}

	files, err := filepath.Glob(filepath.Join(d.outputDir, "*.mp4"))
	if err != nil {
		return "", fmt.Errorf("glob mp4 files: %w", err)
	}
	if len(files) == 0 {
		return "", fmt.Errorf("no .mp4 files found in %s", d.outputDir)
	}
	sort.Strings(files)

	// 逐片校验，损坏分片尝试重新下载，最终无法修复的跳过
	var segmentFiles []string
	var skippedByIndex []int
	for _, f := range files {
		base := filepath.Base(f)
		if strings.HasPrefix(base, "init_") {
			continue
		}

		if err := validateFmp4Segment(f); err != nil {
			Log.Printf("[warn] merge: 分片 %s 校验失败: %v", base, err)

			// 尝试重新下载
			var segIdx int
			if _, scanErr := fmt.Sscanf(base, "%05d.mp4", &segIdx); scanErr == nil && segIdx >= 0 && segIdx < len(d.segments) {
				Log.Printf("[info] merge: 尝试重新下载分片 %d (%s)...", segIdx, base)
				os.Remove(f)
				if dlErr := d.downloadSegment(d.segments[segIdx]); dlErr != nil {
					Log.Printf("[warn] merge: 重新下载分片 %d 失败: %v，跳过", segIdx, dlErr)
					skippedByIndex = append(skippedByIndex, segIdx)
					continue
				}
				// 重新下载后再校验
				if vErr := validateFmp4Segment(f); vErr != nil {
					Log.Printf("[warn] merge: 重新下载后分片 %d 仍无效: %v，跳过", segIdx, vErr)
					skippedByIndex = append(skippedByIndex, segIdx)
					continue
				}
				Log.Printf("[info] merge: 分片 %d 重新下载成功", segIdx)
				segmentFiles = append(segmentFiles, f)
			} else {
				Log.Printf("[warn] merge: 无法定位分片 %s 的 segment 信息，跳过", base)
			}
		} else {
			segmentFiles = append(segmentFiles, f)
		}
	}

	if len(skippedByIndex) > 0 {
		Log.Printf("[warn] merge: 共跳过 %d 个无法修复的损坏分片 (index: %v)，输出视频将缺少对应帧",
			len(skippedByIndex), skippedByIndex)
	}
	if len(segmentFiles) == 0 {
		return "", fmt.Errorf("no valid segment .mp4 files found (all segments corrupted)")
	}

	listPath := filepath.Join(d.outputDir, "concat_list.txt")
	var buf bytes.Buffer
	for _, f := range segmentFiles {
		fmt.Fprintf(&buf, "file '%s'\n", filepath.ToSlash(f))
	}
	if err := os.WriteFile(listPath, buf.Bytes(), 0644); err != nil {
		return "", err
	}
	defer os.Remove(listPath)

	cmd := exec.Command("ffmpeg",
		"-f", "concat",
		"-safe", "0",
		"-i", listPath,
		"-c", "copy",
		"-movflags", "+faststart",
		d.outputFile,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("ffmpeg concat failed: %w\n%s", err, stderr.String())
	}

	return d.outputFile, nil
}
