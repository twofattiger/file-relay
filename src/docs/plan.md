# File Relay Server Implementation Plan

## 目标
在 `server` 目录下实现一个 Go 语言版本的服务端，功能上与 Cloudflare Worker 完全对齐，支持通过 `./file_relay -port 8080` 一键启动。

## 任务拆解

### 1. 项目初始化
- 创建 `server` 目录（如果不存在）。
- 在 `server` 目录下执行 `go mod init file_relay` 初始化模块。
- 添加依赖：`github.com/gorilla/websocket` 用于处理 WebSocket。

### 2. 前端资源提取与内嵌
- 将 `worker.js` 中的 `STYLE`、`LOGIN_HTML`、`SENDER_HTML`、`RECEIVER_HTML` 以及相关 JS 函数直接用 Go 字符串常量存储，或者通过 `go:embed` 嵌入。
- 保证前后端逻辑不需要任何改动即可运行。

### 3. 核心功能实现
#### 3.1 HTTP 路由处理
- `/`：判断是否有合法 Cookie（`sess`），有则返回 `SENDER_HTML`，否则返回 `LOGIN_HTML`。
- `/r/{id}`：返回 `RECEIVER_HTML`。
- `/api/login`：POST 请求，校验密码，成功后设置 `sess` Cookie。
- `/api/logout`：清除 `sess` Cookie。
- `/ws/{id}?role={sender|receiver}`：WebSocket 入口。

#### 3.2 认证机制 (Auth)
- 对齐 Worker 的无状态鉴权算法：`token = timestamp + "." + hmac_sha256(secret, timestamp)`。
- 从请求的 Cookie 中解析并验证。

#### 3.3 中继传输引擎 (类似 Durable Objects)
- 由于是在单机运行，不需要复杂的分布式 DO，仅需在内存中维护一个并发安全的全局 Map `map[string]*TransferSession`，键为 `id`。
- `TransferSession` 需要维护：
  - `Sender` WebSocket 连接
  - `Receiver` WebSocket 连接
  - 线程安全的读写锁（`sync.Mutex` 或 `sync.RWMutex`）
- 处理 WebSocket 的升级、连接绑定和消息转发：
  - 发送方和接收方的消息透传。
  - 一方掉线时发送 `peer-closed` 通知。
  - 第二个相同角色的连接连入时拒绝。

### 4. 命令行参数解析
- 使用 `flag` 包解析命令行参数。
- `-port`：监听端口，默认 8080。
- 支持从环境变量 `PASSWORD` 或命令行参数 `-pwd` 读取密码。如果没有设置，可以使用默认密码或随机生成密码。

### 5. 编译与测试
- 编写 `main.go` 并整合以上模块。
- 编译出 `file_relay` 二进制文件。
- 本地启动测试双端通信和 P2P 打洞 / 中继降级是否正常。
