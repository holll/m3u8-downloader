package dl

import (
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestProxyFromEnv 验证下载器的 transport 会读取 HTTP_PROXY / HTTPS_PROXY 环境变量。
//
// http.ProxyFromEnvironment 的环境变量是进程内 once 缓存（首次调用后固定），
// 无法在同一进程里先设置再验证，故用子进程隔离：父进程带代理环境变量重启自身。
func TestProxyFromEnv(t *testing.T) {
	const subprocFlag = "PROXY_PROBE_SUBPROCESS"

	if os.Getenv(subprocFlag) == "1" {
		runProxyProbe(t)
		return
	}

	const wantProxy = "http://127.0.0.1:7890"

	// 场景一：设置了 HTTPS_PROXY → 必须走代理
	t.Run("设置代理", func(t *testing.T) {
		out, err := runSubprocess(subprocFlag, "HTTPS_PROXY="+wantProxy)
		t.Logf("子进程输出:\n%s", out)
		if err != nil {
			t.Fatalf("子进程失败: %v", err)
		}
		if !strings.Contains(string(out), "PROXY_OK") {
			t.Fatalf("代理未生效，期望 %s", wantProxy)
		}
	})

	// 场景二：显式清空代理变量 → 必须直连
	t.Run("清空代理", func(t *testing.T) {
		out, err := runSubprocess(subprocFlag, "HTTPS_PROXY=", "HTTP_PROXY=")
		t.Logf("子进程输出:\n%s", out)
		if err != nil {
			t.Fatalf("子进程失败: %v", err)
		}
		if !strings.Contains(string(out), "DIRECT_OK") {
			t.Fatal("清空代理变量后仍走了代理")
		}
	})
}

func runSubprocess(flag string, envs ...string) ([]byte, error) {
	cmd := exec.Command(os.Args[0], "-test.run=TestProxyFromEnv", "-test.v")
	cmd.Env = append(os.Environ(), append([]string{flag + "=1"}, envs...)...)
	return cmd.CombinedOutput()
}

// runProxyProbe 在子进程中执行：构造与生产一致的 Downloader，检查其代理决策。
func runProxyProbe(t *testing.T) {
	const target = "https://example.com/playlist.m3u8"

	d := New(Config{M3U8URL: target, MaxWorkers: 8})

	tr, ok := d.reqOpts.HTTPClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport 类型异常: %T", d.reqOpts.HTTPClient.Transport)
	}
	if tr.Proxy == nil {
		t.Fatal("transport.Proxy 为 nil，未读取代理环境变量")
	}

	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	pu, err := tr.Proxy(&http.Request{URL: u})
	if err != nil {
		t.Fatalf("代理决策出错: %v", err)
	}

	if pu == nil {
		t.Logf("SHOULD_DIRECT env  HTTPS_PROXY=%q", os.Getenv("HTTPS_PROXY"))
		t.Log("DIRECT_OK 未走代理（直连）")
		return
	}
	t.Logf("SHOULD_PROXY 代理地址 = %s", pu)
	t.Log("PROXY_OK 走了代理")
}
