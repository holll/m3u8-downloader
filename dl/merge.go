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
		Log.Println("[warn] ffmpeg 未安装，降级为二进制拼接（fMP4 输出大概率损坏！）")
		return d.mergeTS()
	}

	files, err := filepath.Glob(filepath.Join(d.outputDir, "*.mp4"))
	if err != nil {
		return "", fmt.Errorf("glob mp4 files: %w", err)
	}
	if len(files) == 0 {
		return "", fmt.Errorf("no .mp4 files found in %s", d.outputDir)
	}
	sort.Strings(files)

	var segmentFiles []string
	for _, f := range files {
		if !strings.HasPrefix(filepath.Base(f), "init_") {
			segmentFiles = append(segmentFiles, f)
		}
	}
	if len(segmentFiles) == 0 {
		return "", fmt.Errorf("no segment .mp4 files found (only init segments)")
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
