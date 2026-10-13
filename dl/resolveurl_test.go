package dl

import (
	"testing"

	"m3u8-downloader/util"
)

// TestResolveURL m3u8 位于域名根路径时，GetHost 返回的 host 以 "/" 结尾，
// 拼接分片地址不得产生 "//seg.ts"（部分 CDN/nginx 会因此 404）。
func TestResolveURL(t *testing.T) {
	cases := []struct {
		m3u8URL string
		host    string // "" 表示按 v1 从 m3u8URL 推导
		raw     string
		want    string
	}{
		{"http://cdn.test/index.m3u8", "", "seg1.ts", "http://cdn.test/seg1.ts"},
		{"http://cdn.test/index.m3u8", "", "/seg1.ts", "http://cdn.test/seg1.ts"},
		{"http://cdn.test/hls/720p/index.m3u8", "", "seg1.ts", "http://cdn.test/hls/720p/seg1.ts"},
		{"http://cdn.test/hls/720p/index.m3u8", "http://cdn.test", "seg1.ts", "http://cdn.test/seg1.ts"},
		{"http://cdn.test/index.m3u8", "", "https://other.test/a.ts", "https://other.test/a.ts"},
	}

	for _, c := range cases {
		d := &Downloader{}
		if c.host != "" {
			d.m3u8Host = c.host
		} else {
			h, err := util.GetHost(c.m3u8URL, "v1")
			if err != nil {
				t.Fatalf("GetHost(%s): %v", c.m3u8URL, err)
			}
			d.m3u8Host = h
		}
		if got := d.resolveURL(c.raw); got != c.want {
			t.Errorf("resolveURL(%q) with host %q = %q, want %q", c.raw, d.m3u8Host, got, c.want)
		}
	}
}
