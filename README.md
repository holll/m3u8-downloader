# m3u8-downloader

golang 多线程下载直播流m3u8格式的视屏，跨平台。 你只需指定必要的 flag (`u`、`o`、`n`、`ht`) 来运行, 工具就会自动帮你解析 M3U8 文件，并将 TS 片段下载下来合并成一个文件。


## 功能介绍

1. 下载和解析 M3U8
2. 下载 TS 失败重试 （加密的同步解密)
3. 合并 TS 片段
4. 提供 aria2 风格 JSON-RPC API（支持并行任务数控制）

> 可以下载岛国小电影  
> 可以下载岛国小电影  
> 可以下载岛国小电影    
> 重要的事情说三遍......

## 效果展示
![demo](./demo.gif)

## 参数说明：

```
- u  m3u8下载地址(http(s)://url/xx/xx/index.m3u8)
- o  movieName:自定义文件名(默认为movie)不带后缀 (default "movie")
- n  num:下载线程数(默认1)
- ht hostType:设置getHost的方式(v1: http(s):// + url.Host + filepath.Dir(url.Path); v2: `http(s)://+ u.Host` (default "v1")
- c  cookie:自定义请求cookie (例如：key1=v1; key2=v2)
- r  autoClear:是否自动清除ts文件 (default true)
- s  InsecureSkipVerify:是否允许不安全的请求(默认0)
- sp savePath:文件保存的绝对路径(默认为当前路径,建议默认值)(例如：unix:/Users/xxxx ; windows:C:\Documents)
- proxy proxy:下载代理地址(例如：http://127.0.0.1:7890)
- api-listen apiListen:aria2风格JSON-RPC地址(例如 :6800)
- rpc-secret rpcSecret:aria2 rpc鉴权密钥(API模式必填)
- j  jobNum:并行下载任务数(默认1, 仅API模式生效)
- debug 启用调试模式，输出更详细日志到文件 (default false)
```

默认情况只需要传`u`参数,其他参数保持默认即可。 部分链接可能限制请求频率，可根据实际情况调整 `n` 参数的值。

## 下载

已经编译好的平台有： [点击下载](https://github.com/llychao/m3u8-downloader/releases)

- m3u8-darwin-amd64
- m3u8-darwin-arm64
- m3u8-linux-386
- m3u8-linux-amd64
- m3u8-linux-arm64
- m3u8-windows-386.exe
- m3u8-windows-amd64.exe
- m3u8-windows-arm64.exe

## 用法

### 源码方式

```bash
自己编译：go build -o m3u8-downloader
简洁使用：./m3u8-downloader  -u=http://example.com/index.m3u8
完整使用：./m3u8-downloader  -u=http://example.com/index.m3u8 -o=example -n=16 -ht=v1 -c="key1=v1; key2=v2"
代理下载：./m3u8-downloader -u=http://example.com/index.m3u8 -proxy=http://127.0.0.1:7890
调试模式：./m3u8-downloader -u=http://example.com/index.m3u8 -debug  # 日志固定输出到程序同目录 m3u8-downloader.debug.log
```


### API 模式（aria2 风格）

启动 API 服务：

```bash
./m3u8-downloader -api-listen=:6800 -rpc-secret=aword2020 -j=2
```

提交下载任务（`aria2.addUri`）：

```bash
curl -s http://127.0.0.1:6800/jsonrpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"q1","method":"aria2.addUri","params":["token:aword2020",["http://example.com/index.m3u8"],{"out":"movie.mp4","dir":"/tmp","split":"16","all-proxy":"http://127.0.0.1:7890"}]}'
```

查询任务状态（`aria2.tellStatus`）：

```bash
curl -s http://127.0.0.1:6800/jsonrpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"q2","method":"aria2.tellStatus","params":["token:aword2020","<gid>"]}'
```

返回状态包含：`waiting`、`active`、`complete`、`error`。

兼容性增强：已补齐 AriaNg 常用查询接口（如 `aria2.getVersion`、`aria2.getGlobalStat`、`aria2.tellActive`、`aria2.tellWaiting`、`aria2.tellStopped`、`system.multicall`），并同时支持 `/jsonrpc` 与 `/rpc` 路径，支持 `HTTP`/`WebSocket`（`ws://host:port/jsonrpc`）协议；同时要求使用 `token:<rpc-secret>` 鉴权，便于直接对接 ariang 面板。

### 二进制方式:

Linux 和 MacOS 和 Windows PowerShell

```
简洁使用：
./m3u8-linux-amd64 -u=http://example.com/index.m3u8
./m3u8-darwin-amd64 -u=http://example.com/index.m3u8 
.\m3u8-windows-amd64.exe -u=http://example.com/index.m3u8

完整使用：
./m3u8-linux-amd64 -u=http://example.com/index.m3u8 -o=example -n=16 -ht=v1 -c="key1=v1; key2=v2"
./m3u8-darwin-amd64 -u=http://example.com/index.m3u8 -o=example -n=16 -ht=v1 -c="key1=v1; key2=v2"
.\m3u8-windows-amd64.exe -u=http://example.com/index.m3u8 -o=example -n=16 -ht=v1 -c="key1=v1; key2=v2"
```

## 问题说明

1.在Linux或者mac平台，如果显示无运行权限，请用chmod 命令进行添加权限
```bash
 # Linux amd64平台
 chmod 0755 m3u8-linux-amd64
 # Mac darwin amd64平台
 chmod 0755 m3u8-darwin-amd64
 ```
2.下载失败的情况,请设置 -ht="v1" 或者 -ht="v2" （默认为 v1）
```golang
func get_host(Url string, ht string) string {
    u, err := url.Parse(Url)
    var host string
    checkErr(err)
    switch ht {
    case "v1":
        host = u.Scheme + "://" + u.Host + path.Dir(u.Path)
    case "v2":
        host = u.Scheme + "://" + u.Host
    }
    return host
}
```
