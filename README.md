# File Relay (文件实时免落盘中继站)

这是一个超轻量、高性能、**完全不落盘**的文件实时点对点中继站，适用于大文件临时传输、跨设备互传等场景。服务端充当纯粹的“管道”，文件字节流边传边走，不占用任何服务器或云端存储空间。

本项目提供了**两种**独立的服务端实现方式，前端代码完全一致，你可以根据自己的部署环境自由选择：

1. **Go 独立部署版 (`src/server`)**：基于 Go 语言和 Gorilla WebSocket 构建，编译后为一个极小的独立可执行文件，适合自有 VPS、云主机或局域网私有化部署。
2. **Cloudflare Worker 版 (`src/worker`)**：基于 Cloudflare Workers 和 Durable Objects (DO) 构建的 Serverless 版本，适合零成本白嫖 Cloudflare 全球边缘节点部署。

---

## ✨ 核心特性

* **混合传输架构 (WebRTC P2P + 中继兜底)**: 默认优先尝试建立 **WebRTC 点对点直连**，数据不经过服务器，局域网或良好网络环境下传输速度最大化。若由于复杂 NAT/防火墙导致打洞失败，或用户主动关闭 P2P，系统将平滑、无缝地回退到 WebSocket 服务端中继模式。
* **完全免存储 (Zero-Storage)**: 中继模式下，发送方与接收方通过服务端在内存中建立双向 WebSocket 管道配对，字节流实时转发，完全不落盘，无需配置任何数据库或对象存储。
* **高效流控 (Backpressure Control)**: 内置高效的滑动窗口控流机制（8 MiB 传输窗口，每落盘 4 MiB 回传 ACK），有效防止发送方发送过快导致接收方浏览器或服务端内存溢出。
* **原生安全防护**: 传输机制深度绑定浏览器的安全上下文（Secure Context）。强制要求 **HTTPS/WSS** 和 **WebRTC 加密**通道，从根源上杜绝公网明文抓包和中间人攻击。
* **轻量鉴权机制**: 发送端集成基于密码与 `HMAC-SHA256` 算法的无状态签名 Cookie 验证。
* **流式文件保存**: 接收端在支持的浏览器上默认采用 `showSaveFilePicker` API 实时将数据块写入本地磁盘，免去整包下载时的内存积压（不支持的设备将平滑降级为内存分块缓存后下载）。

---

## 🧭 两种使用模式

登录后首页提供两个功能卡片：

### 模式一：分享发送（一对一，单次分享）
发送方选择文件 → 生成一次性链接 `域名/r/{uuid}` 与二维码 → 对方（**无需登录**）打开链接接收。单向、一次一个文件，支持 WebRTC P2P 直连与中继回退。

### 模式二：设备互传（多设备房间 Mesh 组网）
适合「手机 ↔ 电脑（↔ 平板 …）」这类同时在手边、需要互相多次传文件的场景，**支持多台设备同时加入**：
1. 进入 `/room` 创建房间，房间密码默认取当前时间的分秒（如 `0810`），可自定义。
2. 其它设备可通过扫码或在浏览器手动输入链接加入（加入房间均需先登录系统密码）。
3. 每加入一台设备，页面下方就多一张该设备的卡片，可与房间内的每台设备分别**双向、多次**互发文件。

---

## 🛠️ 部署指南

### 选项 A：使用 Go 独立部署 (`src/server`)

> 适合有自己的服务器（Linux/Mac/Windows）或局域网私有化部署。

1. **编译运行**:
   ```bash
   cd src/server
   go mod tidy
   go build -o file-relay-server
   
   # 启动 HTTP/HTTPS 双端口（可结合 Nginx 代理）
   ./file-relay-server -port 8080 -pwd "your_secure_password"
   ```
2. **Nginx 反向代理（推荐）**:
   为了满足浏览器的安全上下文要求（HTTPS），建议使用 Nginx 反向代理 8080 端口并配置 SSL 证书。注意必须配置 WebSocket 升级头 `proxy_set_header Upgrade $http_upgrade;`，以及将超时时间调长 `proxy_read_timeout 86400;` 以防大文件传输中断。
   *(详细文档见 [src/server/README.md](src/server/README.md))*

### 选项 B：部署到 Cloudflare Worker (`src/worker`)

> 适合无自有服务器，希望白嫖 Cloudflare 边缘节点网络的用户。

1. **首次本地部署**:
   由于依赖 Durable Objects，首次必须使用 Wrangler 命令行初始化：
   ```bash
   cd src/worker
   npx wrangler login
   npx wrangler deploy
   ```
2. **配置云端密码与域名**:
   部署成功后，前往 Cloudflare Dashboard，在当前 Worker 的 **Settings -> Variables & Secrets** 中添加名为 `PASSWORD` 的变量作为登录密码。强烈建议绑定自定义域名以避免 `*.workers.dev` 在国内被阻断。
   *(详细文档见 [src/worker/readme.md](src/worker/readme.md))*

---

## 💡 常见问题与注意事项

1. **访问时提示 `crypto.randomUUID is not a function`？**
   Web Crypto API 以及 WebRTC 强依赖浏览器的**安全上下文 (Secure Context)**。请确保你的访问地址是 `https://`（公网）或 `http://localhost` / `http://127.0.0.1`（本地开发），否则浏览器将拒绝工作。
2. **Go 版本如果做多机负载均衡怎么办？**
   当前 Go 版本状态存在于单机内存中，如果前端使用了多机负载均衡器（如 Nginx upstream），**必须开启 IP Hash 或会话保持**，否则发送方和接收方的 WebSocket 可能会连接到不同的机器上导致无法配对。

## 📄 许可证
[MIT License](LICENSE)
