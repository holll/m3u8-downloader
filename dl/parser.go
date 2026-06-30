package dl

import (
	"encoding/hex"
	"fmt"
	"strings"

	"m3u8-downloader/util"

	"github.com/grafov/m3u8"
	"github.com/levigross/grequests"
)

// ============================== M3U8 解析 ==============================

// Parse 下载并解析 m3u8 播放列表
func (d *Downloader) Parse() error {
	resp, err := grequests.Get(d.m3u8URL, d.reqOpts)
	if err != nil {
		return fmt.Errorf("fetch m3u8: %w", err)
	}
	if !resp.Ok {
		return fmt.Errorf("fetch m3u8: HTTP %d", resp.StatusCode)
	}
	d.m3u8Body = resp.String()
	d.m3u8Host = util.GetHost(d.m3u8URL, d.hostType)

	if err := d.parseWithLib(); err != nil {
		Log.Printf("[warn] 库解析失败，降级到手写解析: %v", err)
		return d.parseManual()
	}
	return nil
}

func (d *Downloader) parseWithLib() error {
	playlist, listType, err := m3u8.DecodeFrom(strings.NewReader(d.m3u8Body), true)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	if listType == m3u8.MASTER {
		return fmt.Errorf("检测到 Master Playlist，请使用具体的媒体播放列表 URL")
	}

	mediapl, ok := playlist.(*m3u8.MediaPlaylist)
	if !ok {
		return fmt.Errorf("not a media playlist")
	}

	d.isFmp4 = d.detectFmp4(mediapl)

	for i, seg := range mediapl.Segments {
		if seg == nil {
			continue
		}
		s := Segment{
			Index:    i,
			Duration: seg.Duration,
		}
		s.URL = d.resolveURL(seg.URI)

		// Key（含 IV 解析）
		if seg.Key != nil && seg.Key.Method != "NONE" && seg.Key.URI != "" {
			ki := &KeyInfo{URI: d.resolveURL(seg.Key.URI)}
			if seg.Key.IV != "" {
				ivStr := strings.TrimPrefix(seg.Key.IV, "0x")
				ivStr = strings.TrimPrefix(ivStr, "0X")
				iv, err := hex.DecodeString(ivStr)
				if err == nil && len(iv) == 16 {
					ki.IV = iv
				} else {
					Log.Printf("[warn] segment %d: invalid IV %q, using sequence number", i, seg.Key.IV)
				}
			}
			s.Key = ki
			d.isEncrypted = true
		}

		// Map / fMP4 init segment
		mapRef := seg.Map
		if mapRef == nil {
			mapRef = mediapl.Map
		}
		if mapRef != nil && mapRef.URI != "" {
			s.Map = &MapInfo{
				URI:    d.resolveURL(mapRef.URI),
				Limit:  mapRef.Limit,
				Offset: mapRef.Offset,
			}
		}

		d.segments = append(d.segments, s)
	}

	return nil
}

func (d *Downloader) detectFmp4(pl *m3u8.MediaPlaylist) bool {
	if pl.Map != nil && pl.Map.URI != "" {
		return true
	}
	for _, seg := range pl.Segments {
		if seg != nil && seg.Map != nil && seg.Map.URI != "" {
			return true
		}
	}
	return false
}

func (d *Downloader) parseManual() error {
	lines := strings.Split(d.m3u8Body, "\n")
	index := 0
	var currentKey *KeyInfo

	for _, line := range lines {
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "#EXT-X-MAP") {
			d.isFmp4 = true
			continue
		}

		if strings.Contains(line, "#EXT-X-KEY") {
			if strings.Contains(line, "METHOD=NONE") {
				currentKey = nil
				continue
			}
			if strings.Contains(line, "URI") {
				ki := &KeyInfo{}
				uriStart := strings.Index(line, "URI=\"")
				if uriStart >= 0 {
					uriStart += 5
					uriEnd := strings.Index(line[uriStart:], "\"")
					if uriEnd >= 0 {
						ki.URI = d.resolveURL(line[uriStart : uriStart+uriEnd])
					}
				}
				ivStart := strings.Index(line, "IV=0x")
				if ivStart < 0 {
					ivStart = strings.Index(line, "IV=0X")
				}
				if ivStart >= 0 {
					ivStart += 5
					ivEnd := ivStart
					for ivEnd < len(line) && (('0' <= line[ivEnd] && line[ivEnd] <= '9') ||
						('a' <= line[ivEnd] && line[ivEnd] <= 'f') ||
						('A' <= line[ivEnd] && line[ivEnd] <= 'F')) {
						ivEnd++
					}
					iv, err := hex.DecodeString(line[ivStart:ivEnd])
					if err == nil && len(iv) == 16 {
						ki.IV = iv
					}
				}
				if ki.URI != "" {
					currentKey = ki
					d.isEncrypted = true
				}
			}
			continue
		}

		if !strings.HasPrefix(line, "#") && line != "" {
			index++
			seg := Segment{
				Index: index,
				URL:   d.resolveURL(line),
			}
			if currentKey != nil {
				keyCopy := *currentKey
				seg.Key = &keyCopy
			}
			d.segments = append(d.segments, seg)
		}
	}

	if len(d.segments) == 0 {
		return fmt.Errorf("no segments found in m3u8")
	}
	Log.Println("[info] 使用手写解析模式（部分高级特性可能不可用）")
	return nil
}

func (d *Downloader) resolveURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "http") {
		return raw
	}
	return fmt.Sprintf("%s/%s", d.m3u8Host, strings.TrimPrefix(raw, "/"))
}
