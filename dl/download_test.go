package dl

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateFmp4Segment(t *testing.T) {
	dir := t.TempDir()

	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		return p
	}

	// helper: 构造合法的 ISOBMFF box (size + type + payload)
	makeBox := func(boxType string, payload []byte) []byte {
		size := uint32(8 + len(payload))
		buf := make([]byte, size)
		binary.BigEndian.PutUint32(buf[0:4], size)
		copy(buf[4:8], boxType)
		copy(buf[8:], payload)
		return buf
	}

	t.Run("valid ftyp box", func(t *testing.T) {
		// ftyp box: size=20, minor_version=0, brands="isom"
		payload := make([]byte, 12)
		copy(payload[4:8], "isom")
		p := write("valid_ftyp.mp4", makeBox("ftyp", payload))
		if err := validateFmp4Segment(p); err != nil {
			t.Errorf("expected valid, got: %v", err)
		}
	})

	t.Run("valid moof box (8-byte empty box)", func(t *testing.T) {
		p := write("valid_moof.mp4", makeBox("moof", nil))
		if err := validateFmp4Segment(p); err != nil {
			t.Errorf("expected valid, got: %v", err)
		}
	})

	t.Run("valid mdat box with payload", func(t *testing.T) {
		// mdat 是大数据 box，box size 和文件大小应该匹配
		payload := make([]byte, 1024)
		p := write("valid_mdat.mp4", makeBox("mdat", payload))
		if err := validateFmp4Segment(p); err != nil {
			t.Errorf("expected valid, got: %v", err)
		}
	})

	t.Run("empty file (truncated)", func(t *testing.T) {
		p := write("empty.mp4", []byte{})
		if err := validateFmp4Segment(p); err == nil {
			t.Error("expected error for empty file, got nil")
		} else {
			t.Logf("correctly detected: %v", err)
		}
	})

	t.Run("file too small (< 8 bytes)", func(t *testing.T) {
		p := write("small.mp4", []byte{0x00, 0x00, 0x00, 0x08, 0x6D, 0x6F}) // 只有 6 字节
		if err := validateFmp4Segment(p); err == nil {
			t.Error("expected error for small file, got nil")
		} else {
			t.Logf("correctly detected: %v", err)
		}
	})

	t.Run("garbage data (invalid box type)", func(t *testing.T) {
		// 前 8 字节是垃圾：box type 是 "XXXX"
		garbage := make([]byte, 100)
		binary.BigEndian.PutUint32(garbage[0:4], 100)
		copy(garbage[4:8], "XXXX")
		p := write("garbage.mp4", garbage)
		if err := validateFmp4Segment(p); err == nil {
			t.Error("expected error for garbage box type, got nil")
		} else {
			t.Logf("correctly detected: %v", err)
		}
	})

	t.Run("HTTP response body as garbage", func(t *testing.T) {
		// 模拟收到的是 HTML 或其他非 fMP4 数据（CDN 可能返回错误页面）
		html := []byte(`<html><body>404 Not Found</body></html>`)
		p := write("html.mp4", html)
		if err := validateFmp4Segment(p); err == nil {
			t.Error("expected error for HTML content, got nil")
		} else {
			t.Logf("correctly detected: %v", err)
		}
	})

	t.Run("truncated: box declares larger than file", func(t *testing.T) {
		// 构造 box size=100000，但文件只有 8 字节
		buf := make([]byte, 8)
		binary.BigEndian.PutUint32(buf[0:4], 100000) // 声明 100KB
		copy(buf[4:8], "mdat")                        // 合法 box type
		p := write("truncated.mp4", buf)
		if err := validateFmp4Segment(p); err == nil {
			t.Error("expected error for truncated file (box size > file size), got nil")
		} else {
			t.Logf("correctly detected: %v", err)
		}
	})

	t.Run("truncated mdat: file partially written", func(t *testing.T) {
		// 构造 box size=1032（8 header + 1024 payload），但只写入了 100 字节
		payload := make([]byte, 1024)
		full := makeBox("mdat", payload) // 1032 字节
		truncated := full[:100]          // 只取前 100 字节
		// 头 4 字节声明的 size 仍然是 1032，但文件只有 100 字节
		p := write("truncated_mdat.mp4", truncated)
		if err := validateFmp4Segment(p); err == nil {
			t.Error("expected error for truncated mdat, got nil")
		} else {
			t.Logf("correctly detected: %v", err)
		}
	})

	t.Run("size == 1 (extended 64-bit size) valid", func(t *testing.T) {
		// 扩展 size box：size=1, type="mdat", 8 字节扩展 size = 文件大小
		buf := make([]byte, 16)
		binary.BigEndian.PutUint32(buf[0:4], 1)    // size == 1 表示扩展
		copy(buf[4:8], "mdat")                      // 合法 box type
		binary.BigEndian.PutUint64(buf[8:16], 16)   // 扩展 64-bit size = 文件大小
		p := write("extsize.mp4", buf)
		if err := validateFmp4Segment(p); err != nil {
			t.Errorf("expected valid for extended-size box, got: %v", err)
		}
	})

	t.Run("extended size box truncated", func(t *testing.T) {
		// size == 1 但只有 10 字节（不够 16）
		buf := make([]byte, 10)
		binary.BigEndian.PutUint32(buf[0:4], 1)
		copy(buf[4:8], "mdat")
		p := write("extsize_trunc.mp4", buf)
		if err := validateFmp4Segment(p); err == nil {
			t.Error("expected error for truncated extended-size box, got nil")
		} else {
			t.Logf("correctly detected: %v", err)
		}
	})

	t.Run("size == 0 (runs to EOF) — legal", func(t *testing.T) {
		// size=0 表示 box 延伸到文件末尾 — 合法的最后 box
		payload := make([]byte, 50)
		buf := make([]byte, 8+50)
		binary.BigEndian.PutUint32(buf[0:4], 0) // size=0
		copy(buf[4:8], "mdat")
		copy(buf[8:], payload)
		p := write("zerobox.mp4", buf)
		if err := validateFmp4Segment(p); err != nil {
			t.Errorf("expected valid for size-0 box (to-EOF), got: %v", err)
		}
	})

	t.Run("styp (segment type) box", func(t *testing.T) {
		payload := make([]byte, 12)
		copy(payload[4:8], "msdh") // compatible brand
		p := write("styp.mp4", makeBox("styp", payload))
		if err := validateFmp4Segment(p); err != nil {
			t.Errorf("expected valid styp, got: %v", err)
		}
	})

	t.Run("sidx box valid", func(t *testing.T) {
		p := write("sidx.mp4", makeBox("sidx", make([]byte, 40)))
		if err := validateFmp4Segment(p); err != nil {
			t.Errorf("expected valid sidx, got: %v", err)
		}
	})
}
