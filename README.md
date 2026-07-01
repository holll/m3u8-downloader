# m3u8-downloader

golang 多线程下载直播流 m3u8 视频，跨平台。支持 CLI 单任务模式和 aria2 兼容 RPC Server 模式（可配合 AriaNg 使用）。

## 功能

- 下载和解析 M3U8（TS / fMP4）
- AES-128 加密流解密
- 下载失败自动重试（退避策略）
- **aria2 JSON-RPC Server 模式** — 配合 AriaNg 网页面板管理下载
- **断点续传** — 重启后恢复未完成任务，已有切片自动跳过
- **会话持久化** — `m3u8.session` 保存任务列表，`.mp4.progress` 保存单任务进度
- **配置文件** — 支持 aria2 风格的 `key=value` 配置文件
- **优雅关闭** — Ctrl+C 自动保存会话并释放端口
- **已下载跳过** — 输出文件已存在时自动跳过，不消耗网络请求

## 快速开始

### CLI 模式（单任务）

```powershell
# 最简用法
.\m3u8-downloader.exe -u=https://example.com/index.m3u8

# 完整参数
.\m3u8-downloader.exe -u=https://example.com/index.m3u8 -o=myvideo -n=16 -ht=v2 -c="key=val"
```

### RPC Server 模式（AriaNg 面板）

```powershell
# 启动服务
.\m3u8-downloader.exe -rpc-listen-port=6800 -rpc-secret=your_token

# 浏览器打开 AriaNg，连接到 http://127.0.0.1:6800/jsonrpc
```

### 使用配置文件

```powershell
# 复制示例配置
copy m3u8.conf.example m3u8.conf

# 编辑后启动
.\m3u8-downloader.exe --conf-path=m3u8.conf

# CLI 参数可覆盖配置文件中的值
.\m3u8-downloader.exe --conf-path=m3u8.conf -rpc-secret=override
```

---

## 参数说明

### CLI 模式参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `-u` | string | — | m3u8 下载地址 |
| `-o` | string | `movie` | 输出文件名（不含扩展名） |
| `-n` | int | 3 | 下载线程数 |
| `-ht` | string | `v1` | host 拼接策略：`v1`=scheme://host+path目录，`v2`=scheme://host |
| `-c` | string | — | 自定义 Cookie（格式：`key1=v1; key2=v2`） |
| `-r` | bool | `true` | 完成后是否清除临时 ts 文件 |
| `-s` | int | 0 | 跳过 HTTPS 证书校验（1=跳过） |
| `-sp` | string | — | 文件保存路径（绝对路径，默认当前目录） |

### RPC Server 模式参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `-rpc-listen-port` | int | 0 | RPC 监听端口（0=CLI模式，非0=Server模式） |
| `-rpc-secret` | string | — | RPC 鉴权密钥（空=无鉴权，不推荐） |
| `-rpc-listen-all` | bool | `false` | 监听所有网卡（`false` 仅监听 127.0.0.1） |
| `-max-concurrent-downloads` | int | 1 | 最大同时下载任务数 |
| `-session-file` | string | `m3u8.session` | 会话文件路径，重启后恢复未完成任务 |
| `--conf-path` | string | — | 配置文件路径（key=value 格式） |

---

## 配置文件

支持 aria2 风格的配置文件（`key=value`，`#` 注释）：

```ini
# m3u8.conf
rpc-listen-port=6800
rpc-secret=mysecret
max-concurrent-downloads=5
n=8
session-file=m3u8.session
```

优先级：**CLI 参数 > 配置文件 > 默认值**

---

## 会话与断点续传

### m3u8.session

自动保存未完成（active/waiting/paused）的任务列表。每次状态变更时异步写入，Ctrl+C 时同步最终保存。

```json
[
  {
    "gid": "a1b2c3d4...",
    "url": "https://example.com/index.m3u8",
    "dir": ".",
    "out": "myvideo",
    "maxWorkers": 8,
    "status": "active",
    "totalSegments": 128,
    "completedSegments": 45,
    "bytesReceived": 47185920
  }
]
```

### {out}.mp4.progress

位于输出目录，命名为 `{文件名}.mp4.progress`。每 20 个切片更新一次，重启恢复时优先使用此文件中的进度值。

> 例如视频为 `/data/1.mp4`，则进度文件为 `/data/1.mp4.progress`。

### 恢复流程

```
重启 → 加载 m3u8.session → 恢复任务 → 重新解析 m3u8
     → 下载阶段：已有切片文件跳过 → 合并 → 完成
```

---

## 用法示例

### CLI 模式

```bash
# Linux / macOS
chmod 0755 m3u8-linux-amd64
./m3u8-linux-amd64 -u=http://example.com/index.m3u8

# Windows
.\m3u8-windows-amd64.exe -u=http://example.com/index.m3u8
```

### RPC Server 模式

```bash
# 基础启动
./m3u8-linux-amd64 -rpc-listen-port=6800 -rpc-secret=my_token

# 外网访问 + 配置 + 多任务
./m3u8-linux-amd64 \
  -rpc-listen-port=6800 \
  -rpc-secret=my_token \
  -rpc-listen-all=true \
  -max-concurrent-downloads=5 \
  -session-file=/data/m3u8.session \
  --conf-path=/etc/m3u8.conf
```

### 配合 AriaNg

1. 启动 RPC Server
2. 打开 [AriaNg](http://ariang.mayswind.net/)
3. 设置 → RPC → 填入地址 `http://127.0.0.1:6800/jsonrpc` 和密钥
4. 新建任务，粘贴 m3u8 地址即可

---

## 编译

```bash
go build -o m3u8-downloader .
```

## 下载

预编译二进制：[Releases](https://github.com/llychao/m3u8-downloader/releases)

- m3u8-darwin-amd64 / m3u8-darwin-arm64
- m3u8-linux-386 / m3u8-linux-amd64 / m3u8-linux-arm64
- m3u8-windows-386.exe / m3u8-windows-amd64.exe / m3u8-windows-arm64.exe

## 常见问题

1. **下载失败** — 尝试切换 host 拼接策略：`-ht=v2`
2. **fMP4 流合并失败** — 需要安装 ffmpeg 并确保在 PATH 中
3. **端口占用** — 程序退出后端口自动释放（已启用 SO_REUSEADDR）；如仍有占用，可使用 `netstat -ano | findstr :6800` 查找进程并终止
4. **Linux/macOS 权限** — `chmod 0755 m3u8-linux-amd64`
