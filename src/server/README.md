# Go File Relay (文件实时免落盘中继站 - Go 独立部署版)

这是一个基于 Go 语言和 Gorilla WebSocket 构建的超轻量、高性能、**完全不落盘**的文件实时点对点中继站。

本版本为 **独立服务器版本**，适用于自有 VPS、云主机或局域网内的私有化部署。与 Cloudflare Worker 版本类似，服务端仅充当纯粹的“管道”，文件字节流边传边走，不占用任何服务器磁盘存储空间。

## ✨ 核心特性

* **混合传输架构 (WebRTC P2P + 中继)**: 默认优先尝试建立 **WebRTC 点对点直连**，数据不经过服务器，局域网或良好网络环境下传输速度最大化。若由于复杂 NAT/防火墙导致打洞失败，或用户主动关闭 P2P，系统将平滑、无缝地回退到 Go WebSocket 服务端中继模式。
* **完全免存储 (Zero-Storage)**: 无需配置任何数据库或外部缓存。中继模式下，发送方与接收方在 Go 进程的内存中通过 `sync.Mutex` 和 Channel 建立双向 WebSocket 管道配对，字节流实时转发，完全不落盘。
* **极简部署 (Single Binary)**: 纯 Go 编写，编译后为一个极小的独立可执行文件。无复杂依赖，即拷即用。
* **高效流控 (Backpressure Control)**: 内置高效的滑动窗口控流机制（中继模式 512 KiB 分块 / 直连模式 256 KiB 分块，8 MiB 传输窗口，每落盘 4 MiB 回传 ACK），有效防止发送方发送过快导致接收方浏览器或服务端内存溢出。
* **原生安全防护**: 传输机制深度绑定浏览器的安全上下文（Secure Context）。强制要求 **HTTPS/WSS** 和 **WebRTC 加密**通道，从根源上杜绝网络抓包。
* **轻量鉴权机制**: 发送端集成基于密码与 `HMAC-SHA256` 算法的无状态签名 Cookie 验证。
* **流式文件保存**: 接收端在支持的浏览器上默认采用 `showSaveFilePicker` API 实时将数据块写入本地磁盘，免去整包下载时的内存积压。

## 🌐 传输协议规范

两端通过专属的 `transfer_id` 连接到同一个 Go 服务端实例。连接后双方首先交换传输偏好，如果双方都允许 P2P，则通过服务端交换 WebRTC 信令（SDP/ICE）；若连接成功则走 DataChannel 直传，否则自动回退通过 WebSocket 中继转发二进制流：

```text
发送方 (Sender)                                接收方 (Receiver)
       |                                              |
       | --------- {type:"prefs", p2p:true} --------> | (交换偏好)
       | <-------- {type:"prefs", p2p:true} --------- |
       |                                              |
       | ====== [可选] WebRTC 信令交换 (SDP/ICE) ==== |
       |                                              |
       | -- {type:"meta", name, size, chunkSize...}-> | (通过直连通道或WS中继)
       |                                              | (点击选择保存位置)
       | <----------------- {type:"ready"} ---------- |
       |                                              |
       | ================= 开始推流 ================= |
       | ------ [Binary Chunk 1 (256/512 KiB)] -----> |
       | ------ [Binary Chunk 2 (256/512 KiB)] -----> |
       |                                              | (累计落盘 4 MiB)
       | <---- {type:"ack", bytes: 4194304} --------- | (滑动窗口向前滚动)
       | ------ [Binary Chunk n (256/512 KiB)] -----> |
       | ------ {type:"eof"} -----------------------> |
       |                                              | (保存并关闭文件描述符)
       | <---------------- {type:"complete"} -------- |
```

---

## 🛠️ 部署指南

### 前置条件

* 本地或服务器已安装 [Go 环境](https://go.dev/doc/install) (建议 Go 1.18+)。
* 如果需要在公网使用，必须准备一个域名，并配置反向代理（如 Nginx、Caddy）以提供 HTTPS 证书。

### 步骤一：编译项目

在 `server` 目录下，获取依赖并编译可执行文件：

```bash
cd src/server
go mod tidy
go build -o file-relay-server
```

### 步骤二：运行服务

你可以直接运行编译后的文件。服务端支持通过命令行参数或环境变量配置参数。

**参数说明**：
* `-port`: 监听的本地端口（默认 `8080`）
* `-pwd`: 发送端登录鉴权密码（若不填写，系统会尝试读取 `PASSWORD` 环境变量。若均为空，则发送端将**不设密码，完全开放**）。

**运行示例**：
```bash
# 指定端口和密码运行
./file-relay-server -port 8080 -pwd "your_secure_password"

# 或者使用环境变量
export PASSWORD="your_secure_password"
./file-relay-server -port 8080
```

### 步骤三：配置反向代理 (强烈推荐)

> **⚠️ 重要提示**：由于浏览器的安全限制，WebRTC 信令交互和原生本地文件系统 API (`showSaveFilePicker`) **必须在安全上下文 (Secure Context，即 HTTPS) 下才能运行**。如果是公网部署，务必配置反向代理开启 HTTPS。

#### Nginx 参考配置：
```nginx
server {
    listen 443 ssl;
    server_name relay.yourdomain.com;

    ssl_certificate /path/to/your/fullchain.cer;
    ssl_certificate_key /path/to/your/cert.key;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # WebSocket 必须特殊配置代理支持
    location /ws/ {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400; # 防止长连接超时中断
    }
}
```

---

## 📝 踩坑与运维重点记录

### 1. 单机状态限制 (Single-node State)
与 Cloudflare Durable Objects 版本的分布式特性不同，当前 Go 版本的会话状态完全保存在单机进程的内存（Map）中。**如果你使用了多台服务器做负载均衡，必须开启“IP 哈希 (IP Hash) 或会话保持”**，确保同一个发送方和接收方的 WebSocket 连接落在同一个 Go 进程上，否则双方将无法完成匹配。

### 2. 访问时提示 `crypto.randomUUID is not a function`？
这是浏览器的标准安全限制。Web Crypto API 仅在**安全上下文 (Secure Context)** 中生效。
* **解决办法**：请检查浏览器地址栏，确保使用的是 `https://` 协议，或者在本地开发时使用 `http://localhost` / `http://127.0.0.1` 访问。

### 3. 连接超时或中途断开？
传输过程强依赖 WebSocket 长连接。如果使用了 Nginx 作为前置代理，默认的 `proxy_read_timeout` 通常只有 60 秒。大文件传输（或长时间挂机）极易被 Nginx 掐断。**请务必在 Nginx 的 `/ws/` location 中配置足够长的超时时间（如 `proxy_read_timeout 86400;`）。**

---

## 📄 开源许可证

[MIT License]
