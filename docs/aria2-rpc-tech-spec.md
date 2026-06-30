# aria2 JSON-RPC 兼容接口 — 技术方案

> 分支: `feat/aria2-rpc`  
> 日期: 2026-06-30  
> 目标: 提供 aria2 兼容的 JSON-RPC 2.0 接口，使 AriaNg 等 Web 管理工具可直接管理 m3u8 下载任务

---

## 1. 架构总览

```
┌──────────────────────────────────────────────┐
│                AriaNg (浏览器)                 │
│          http://ariang.example.com            │
└──────────────────┬───────────────────────────┘
                   │ POST /jsonrpc
                   │ (JSON-RPC 2.0 over HTTP)
┌──────────────────┴───────────────────────────┐
│              rpc 包 (新建)                     │
│  ┌─────────┐  ┌──────────┐  ┌─────────────┐  │
│  │ server  │  │  types   │  │   methods   │  │
│  │ HTTP    │  │ 请求/响应 │  │ aria2.*     │  │
│  │ CORS    │  │ 错误码   │  │ 实现        │  │
│  │ Token   │  └──────────┘  └──────┬──────┘  │
│  └────┬────┘                       │         │
│       │                            ▼         │
│       │              ┌─────────────────────┐  │
│       └──────────────┤    task 包 (新建)    │  │
│                      │   Manager           │  │
│                      │  ┌───────┐┌───────┐ │  │
│                      │  │ Task#1││ Task#2│ │  │
│                      │  └───┬───┘└───┬───┘ │  │
│                      └──────┼────────┼─────┘  │
│                             │        │        │
│                      ┌──────┴──┐ ┌───┴──────┐ │
│                      │  dl 包  │ │  dl 包   │ │
│                      │(已存在)  │ │ (已存在)  │ │
│                      └─────────┘ └──────────┘ │
└──────────────────────────────────────────────┘
```

### 调用链

```
AriaNg POST
  → rpc/server 鉴权/解析 JSON-RPC
    → rpc/methods 适配参数
      → task/Manager 路由到具体 Task
        → dl.Downloader 执行下载
          → 回调 → task/Task 更新状态
            → 轮询时通过 rpc/methods 序列化返回
```

---

## 2. 包结构设计

新增两个包，修改一个包（`dl`）和一个入口文件（`main.go`）：

```
m3u8-downloader/
├── main.go                          ← 修改：双模式入口
├── dl/                              ← 修改：context + 回调
│   ├── types.go                     StatusSnapshot 结构体
│   ├── dl.go                        context 注入 + 回调注册
│   ├── parser.go                    无变化
│   ├── download.go                  context 检查点 + 字节回调
│   └── merge.go                     context 检查点
├── task/                            ← 新建
│   ├── task.go                      Task 状态机
│   ├── manager.go                   Manager 任务注册表
│   └── status.go                    Status 枚举 + TaskStatus (aria2 JSON)
├── rpc/                             ← 新建
│   ├── server.go                    HTTP Server + 中间件
│   ├── types.go                     JSON-RPC 2.0 协议类型
│   └── methods.go                   aria2.* 方法实现
├── crypto/                          (不变)
├── util/                            (不变)
└── docs/
    └── aria2-rpc-tech-spec.md       本文档
```

---

## 3. 详细设计

### 3.1 `task/status.go` — 状态枚举

```go
type Status int

const (
    StatusWaiting  Status = iota  // 排队等待槽位
    StatusActive                  // 正在下载
    StatusPaused                  // 用户暂停
    StatusError                   // 下载失败
    StatusComplete                // 下载完成（已合并）
    StatusRemoved                 // 已删除
)
```

### 3.2 `task/task.go` — Task 结构体与状态机

```go
type Task struct {
    GID        string          // 16 位 hex 字符串
    Status     Status
    StatusMu   sync.RWMutex

    // 用户选项 (来自 aria2.addUri 的 options)
    Dir        string          // 保存目录
    Out        string          // 输出文件名 (不含后缀)
    Cookie     string          // 自定义 Cookie
    MaxWorkers int             // 并发线程数

    // dl.Downloader 实例 (在 Start 时创建)
    dl         *dl.Downloader

    // 统计信息
    TotalLength      int64     // 估算总字节数
    CompletedLength  int64     // 已下载字节数 (atomic)
    DownloadSpeed    int64     // 瞬时速度 B/s (atomic)
    ErrorMessage     string
    ErrorCode        string

    // 分段信息 (aria2 兼容)
    Files       []FileInfo     // 下载的文件列表
    NumPieces   int64          // 切片数 (segment count)
    InfoHash    string         // 留空

    // 时间戳
    CreatedAt   time.Time
    StartedAt   time.Time
    FinishedAt  time.Time

    // 内部状态
    cancel      context.CancelFunc
    onUpdate    func()         // 通知 Manager 状态变更
}

type FileInfo struct {
    Index           string  `json:"index"`           // "1"
    Path            string  `json:"path"`            // 完整输出路径
    Length          string  `json:"length"`          // 文件总大小
    CompletedLength string  `json:"completedLength"` // 已下载大小
    Selected        string  `json:"selected"`        // "true"
    URIs            []URIInfo `json:"uris"`           // 空数组
}

type URIInfo struct {
    URI    string `json:"uri"`
    Status string `json:"status"` // "used"
}
```

**状态转移：**

```
                    addUri()
                       │
                       ▼
                  ┌─ Waiting ──┐
                  │  (排队中)    │
                  └──────┬─────┘
              槽位释放    │
                         ▼
                  ┌─ Active ───┐
                  │  (下载中)    │
                  └──┬───┬─────┘
           pause()  │   │  unpause()
                    ▼   │
               ┌─ Paused ───┐
               │  (暂停)     │──────► Active
               └────────────┘

        下载完成/失败
              │
    ┌─────────┴─────────┐
    ▼                   ▼
  Complete            Error
    │                   │
    └───────┬───────────┘
            │ remove()
            ▼
          Removed
```

**关键方法签名：**

```go
func NewTask(gid, url string, opts TaskOptions, onUpdate func()) *Task

func (t *Task) Start(manager *Manager)     // 开始执行（在独立 goroutine 中）
func (t *Task) Pause()
func (t *Task) Unpause()
func (t *Task) Remove()
func (t *Task) Snapshot() TaskSnapshot     // 线程安全的当前状态快照
```

**Start 流程（Task 核心引擎）：**

```go
func (t *Task) Start(mgr *Manager) {
    t.SetStatus(StatusActive)

    // Phase 1: Parse
    d := dl.New(dl.Config{...})
    d.SetBytesCallback(func(n int64) {
        atomic.AddInt64(&t.CompletedLength, n)
    })
    d.SetProgressCallback(func(done, total int64) {
        t.NumPieces = total
        t.onUpdate()
    })

    if err := d.Parse(); err != nil {
        t.SetError(err)
        return
    }

    // Phase 2: Download
    if err := d.DownloadAll(); err != nil {
        t.SetError(err)
        return
    }

    // Phase 3: Merge
    if _, err := d.Merge(); err != nil {
        t.SetError(err)
        return
    }

    t.SetStatus(StatusComplete)
}
```

### 3.3 `task/manager.go` — Manager

```go
type Manager struct {
    tasks         map[string]*Task       // GID → Task
    mu            sync.RWMutex
    activeLimit   int                    // 最大并发下载数
    activeCount   int                    // 当前 active 数
    waitingQueue  []string               // GID 队列
    secret        string                 // RPC 鉴权 token (空 = 无需鉴权)
}
```

**关键方法：**

```go
func NewManager(maxConcurrent int, secret string) *Manager

func (m *Manager) AddURI(url string, opts TaskOptions) (string, error)
func (m *Manager) Remove(gid string) error
func (m *Manager) Pause(gid string) error
func (m *Manager) Unpause(gid string) error
func (m *Manager) Status(gid string) (TaskSnapshot, error)
func (m *Manager) TellActive() []TaskSnapshot
func (m *Manager) TellWaiting(offset, num int) []TaskSnapshot
func (m *Manager) TellStopped(offset, num int) []TaskSnapshot
func (m *Manager) GlobalStat() GlobalStat
func (m *Manager) RemoveDownloadResult(gid string) error
```

**并发控制逻辑：**

```
AddURI()
  ├─ 创建 Task，存入 tasks map
  ├─ 若 activeCount < activeLimit → Start(task) → activeCount++
  └─ 否则 → waitingQueue.Append(gid)

Task 完成/失败/暂停:
  └─ 若该 Task 是 active → activeCount--
     └─ 若 waitingQueue 非空 → 弹出第一个 GID → Start(task) → activeCount++
```

### 3.4 `rpc/types.go` — JSON-RPC 2.0 类型

```go
type Request struct {
    JSONRPC string          `json:"jsonrpc"`  // 必须为 "2.0"
    Method  string          `json:"method"`   // "aria2.addUri"
    Params  json.RawMessage `json:"params"`   // 参数数组 (或对象)
    ID      json.RawMessage `json:"id"`       // 请求 ID (回传)
}

type Response struct {
    JSONRPC string      `json:"jsonrpc"`
    Result  interface{} `json:"result,omitempty"`
    Error   *RPCError   `json:"error,omitempty"`
    ID      interface{} `json:"id"`
}

type RPCError struct {
    Code    int    `json:"code"`
    Message string `json:"message"`
}

// 标准 JSON-RPC 错误码
const (
    ErrParse      = -32700
    ErrInvalidReq = -32600
    ErrMethod     = -32601
    ErrParams     = -32602
    ErrInternal   = -32603
)
```

### 3.5 `rpc/server.go` — HTTP Server

```go
type Server struct {
    manager *task.Manager
    secret  string
    addr    string
}

func NewServer(addr string, mgr *task.Manager) *Server

func (s *Server) Start() error       // 启动 HTTP server，阻塞
func (s *Server) Stop() error        // 优雅关闭

// HTTP Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request)
    ├─ 检查 Method == POST && Path == "/jsonrpc"
    ├─ 鉴权 (token:secret 参数)
    ├─ 解析 JSON body → Request
    ├─ 路由方法名 → handler
    ├─ 执行 handler → Response
    └─ JSON 序列化 + CORS 头 → 返回
```

**中间件链：**
1. CORS 头注入（`*` 或可配置 Origin）
2. Token 鉴权（若 secret 非空，验证 `r.URL.Query().Get("token")` 或 `Authorization` header）
3. 请求日志（可选）

### 3.6 `rpc/methods.go` — aria2 方法实现

每个方法一个 handler 函数，签名统一为：

```go
type MethodHandler func(params json.RawMessage, mgr *task.Manager) (interface{}, error)
```

注册表：

```go
var methodTable = map[string]MethodHandler{
    "aria2.addUri":               handleAddUri,
    "aria2.tellStatus":           handleTellStatus,
    "aria2.tellActive":           handleTellActive,
    "aria2.tellWaiting":          handleTellWaiting,
    "aria2.tellStopped":          handleTellStopped,
    "aria2.remove":               handleRemove,
    "aria2.removeDownloadResult": handleRemoveDownloadResult,
    "aria2.pause":                handlePause,
    "aria2.unpause":              handleUnpause,
    "aria2.getGlobalStat":        handleGetGlobalStat,
    "aria2.getVersion":           handleGetVersion,
    "aria2.getSessionInfo":       handleGetSessionInfo,
    "system.multicall":           handleMulticall,
}
```

### 3.7 aria2 响应格式兼容映射

AriaNg 期望的 TaskStatus JSON（核心字段）：

```json
{
  "gid":             "d4e5f6a7b8c9d0e1",
  "status":          "active",
  "totalLength":     "104857600",
  "completedLength": "52428800",
  "downloadSpeed":   "2097152",
  "uploadSpeed":     "0",
  "uploadLength":    "0",
  "connections":     "16",
  "errorCode":       "0",
  "errorMessage":    "",
  "dir":             "/downloads",
  "files": [{
    "index":           "1",
    "path":            "/downloads/video.mp4",
    "length":          "104857600",
    "completedLength": "52428800",
    "selected":        "true",
    "uris": [{"uri": "http://.../index.m3u8", "status": "used"}]
  }],
  "bittorrent": {},
  "infoHash":        "",
  "numPieces":       "300",
  "pieceLength":     "349525"
}
```

**重要约束：** aria2 所有数值字段必须是**字符串**类型，否则 AriaNg 解析异常。

### 3.8 `main.go` — 双模式入口

```go
var (
    // 现有 CLI flag (不变)
    urlFlag  = flag.String("u", "", "...")
    nFlag    = flag.Int("n", 24, "...")
    // ...

    // 新增 RPC flag
    rpcPort     = flag.Int("rpc-listen-port", 0,
        "RPC 监听端口 (0=CLI模式, 非0=Server模式, 默认6800)")
    rpcSecret   = flag.String("rpc-secret", "",
        "RPC 鉴权 token (空=无鉴权)")
    rpcListen   = flag.String("rpc-listen-all", "false",
        "是否监听所有网卡 (默认仅 localhost)")
    maxDownload = flag.Int("max-concurrent-downloads", 5,
        "最大同时下载数 (Server模式)")
)

func Run() {
    flag.Parse()

    if *rpcPort != 0 {
        runServer()
    } else {
        runCLI()
    }
}

func runServer() {
    mgr := task.NewManager(*maxDownload, *rpcSecret)
    addr := fmt.Sprintf("127.0.0.1:%d", *rpcPort)
    if *rpcListen == "true" {
        addr = fmt.Sprintf("0.0.0.0:%d", *rpcPort)
    }
    srv := rpc.NewServer(addr, mgr)
    fmt.Printf("[RPC] 服务已启动: http://%s/jsonrpc\n", addr)
    log.Fatal(srv.Start())
}

func runCLI() {
    // 现有 CLI 逻辑完全不变
}
```

---

## 4. `dl` 包改造清单

### 4.1 新增回调机制 (dl/types.go)

```go
// ProgressTracker 新增
type ProgressTracker struct {
    // ...existing...
    onProgress func(completed, total int64)
    onBytes    func(n int64)
}

// Downloader 新增方法
func (d *Downloader) OnProgress(fn func(completed, total int64))
func (d *Downloader) OnBytes(fn func(n int64))
```

### 4.2 download.go 集成点

```go
// downloadSingle() 中，resp.Bytes() 之后：
if d.progress != nil {
    d.progress.AddBytes(int64(len(data)))     // 已有
    if d.progress.onBytes != nil {
        d.progress.onBytes(int64(len(data)))  // 新增
    }
}

// downloadSegment() 中，成功返回前：
if d.progress != nil && d.progress.onProgress != nil {
    d.progress.onProgress(
        d.progress.Completed(),
        d.progress.Total(),
    )
}
```

### 4.3 无需 context 支持

当前设计中，Task 的暂停通过 `Status` 状态机 + goroutine 不启动新任务实现，不需要在 `dl.Downloader` 内部注入 context。已启动的 Active 任务执行到完成，暂停只影响新任务从 Waiting 到 Active 的调度。

**简化理由：** m3u8 下载的特点是解析 → 下载 → 合并，其中下载阶段（DownloadAll）由数百个独立 HTTP 请求组成。如果在 DownloadAll 中途暂停，已经下载的 segment 文件需要保留（断点续传），暂停恢复后只需继续下载剩余 segment。当前代码已支持断点续传（`downloadSegment` 检查文件是否存在），所以 context 注入可推迟到后续迭代。

---

## 5. 测试策略

| 层级 | 测试内容 | 工具 |
|------|---------|------|
| `task` 单元测试 | 状态转移、并发控制、Active/Waiting 队列 | `go test` |
| `rpc` 单元测试 | JSON-RPC 协议解析、错误码、鉴权 | `httptest.NewServer` |
| `rpc` + `task` 集成测试 | addUri → tellStatus 完整链路 | `httptest` + mock dl |
| 端到端测试 | 启动 server，AriaNg 连接，添加真实 m3u8 URL | 手动 |

---

## 6. 命令行用法

### RPC Server 模式

```bash
# 启动 (仅本地访问)
./m3u8-downloader -rpc-listen-port=6800

# 带鉴权
./m3u8-downloader -rpc-listen-port=6800 -rpc-secret=mysecret

# 允许远程访问
./m3u8-downloader -rpc-listen-port=6800 -rpc-listen-all=true

# 限制并发
./m3u8-downloader -rpc-listen-port=6800 -max-concurrent-downloads=3
```

### AriaNg 连接设置

```
AriaNg → 设置 → RPC
  - 地址: http://127.0.0.1:6800/jsonrpc
  - 密钥: mysecret (与 -rpc-secret 一致)
```

### CLI 模式（不变）

```bash
./m3u8-downloader -u=http://example.com/index.m3u8 -o=movie -n=24
```

---

## 7. 实施步骤

| 步骤 | 内容 | 文件 | 验证 |
|------|------|------|------|
| 1 | 创建 `task/status.go` | Status 枚举 + TaskStatus JSON 类型 | `go build ./...` |
| 2 | 创建 `task/task.go` | Task 结构体 + 状态机骨架 (Start 用 sleep 模拟) | 单元测试 |
| 3 | 创建 `task/manager.go` | Manager + AddURI/Remove/并发控制 | 单元测试 |
| 4 | 创建 `rpc/types.go` | JSON-RPC 协议类型 + 错误码 | `go vet` |
| 5 | 创建 `rpc/methods.go` | 全部 aria2.* handler + system.multicall | httptest |
| 6 | 创建 `rpc/server.go` | HTTP Server + CORS + Token | `curl` 测试 |
| 7 | 改造 `dl/types.go`, `dl/dl.go` | OnProgress/OnBytes 回调注册 | CLI 不受影响 |
| 8 | 改造 `dl/download.go` | 字节回调触发 | CLI 不受影响 |
| 9 | 集成 task ↔ dl | Task.Start() 驱动真实 Downloader | RPC 下载单文件 |
| 10 | 改造 `main.go` | 双模式入口 | server 模式 + CLI 模式 |
| 11 | AriaNg 联调 | 真实浏览器连接测试 | 手动 |
| 12 | 移除 mock/fixme + 完善文档 | 清理代码 | `go test ./...` |

---

## 8. 风险与缓解

| 风险 | 缓解 |
|------|------|
| dl.Downloader 的回调在并发 goroutine 中触发，Task 状态读写需加锁 | `sync.RWMutex` + `atomic` 保护统计字段 |
| AriaNg 严格校验 JSON 字段类型（数值必须字符串） | 所有数值字段用 `json:"...,string"` tag |
| Manager 的 activeCount 与 Task 实际状态可能漂移 | 状态变更统一走 Manager 方法，不做外部直接修改 |
| 大量 Task 的 waiting 队列内存占用 | 设置 `--max-concurrent-downloads` 合理上限 |
| fMP4 需要 ffmpeg，Server 环境可能缺失 | 启动时检测并打印警告，降级同 CLI 行为 |
