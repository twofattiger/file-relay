
# Cloudflare Worker File Relay (文件实时免落盘中继站)

这是一个基于 Cloudflare Workers 和 Durable Objects (DO) 构建的超轻量、高性能、**完全不落盘**的文件实时点对点中继站。

适用于大文件临时传输、跨设备互传等场景。服务端充当纯粹的“管道”，文件字节流边传边走，不占用任何公网云端存储空间。

## ✨ 核心特性

* **混合传输架构 (WebRTC P2P + 中继)**: 默认优先尝试建立 **WebRTC 点对点直连**，数据不经过服务器，局域网或良好网络环境下传输速度最大化。若由于复杂 NAT/防火墙导致打洞失败，或用户主动关闭 P2P，系统将平滑、无缝地回退到 Cloudflare WebSocket 服务端中继模式。
* **完全免存储 (Zero-Storage)**: 无需配置 Cloudflare R2、KV 或 D1 数据库。中继模式下，发送方与接收方通过 Durable Object 建立的双向 WebSocket 管道配对，字节流在内存中实时转发，完全不落盘。
* **高效流控 (Backpressure Control)**: 内置高效的滑动窗口控流机制（中继模式 512 KiB 分块 / 直连模式 256 KiB 分块，8 MiB 传输窗口，每落盘 4 MiB 回传 ACK），有效防止发送方发送过快导致接收方浏览器或服务端内存溢出。
* **原生安全防护**: 传输机制深度绑定浏览器的安全上下文（Secure Context）。强制要求 **HTTPS/WSS** 和 **WebRTC 加密**通道，从根源上杜绝公网明文抓包和中间人攻击。
* **轻量鉴权机制**: 发送端集成基于密码与 `HMAC-SHA256` 算法的无状态签名 Cookie 验证。
* **流式文件保存**: 接收端在支持的浏览器上默认采用 `showSaveFilePicker` API 实时将数据块写入本地磁盘，免去整包下载时的内存积压（不支持的设备将平滑降级为内存分块缓存后下载）。

## 🌐 传输协议规范

两端通过专属的 `transfer_id` 分配到同一个 Durable Object 实例中。连接后双方首先交换传输偏好，如果双方都允许 P2P，则通过 DO 交换 WebRTC 信令（SDP/ICE）；若连接成功则走 DataChannel 直传，否则自动回退通过 DO 中继转发二进制流：

```
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

## 🧭 两种使用模式

登录后首页提供两个功能卡片：

### 模式一：分享发送（`/send`，原有逻辑，未改动）

发送方选择文件 → 生成一次性链接 `域名/r/{uuid}` 与二维码 → 对方（**无需登录**）打开链接接收。单向、一次一个文件，支持 WebRTC P2P 直连与 CF 中继回退。

### 模式二：设备互传（多设备房间，`/m/{密码}`）

适合「手机 ↔ 电脑（↔ 平板 …）」这类同时在手边、需要互相多次传文件的场景，**支持多台设备同时加入**：

1. 进入 `/room` 创建房间，房间密码默认取**当前时间的分秒**（如 `0810`），可自定义；
2. 生成后进入 `域名/m/{密码}`，页面顶部显示二维码、链接和**设备名称输入框**；
3. 其它设备可**扫码**，也可在浏览器**手动输入** `域名/m/0810` 加入（密码为 4 位纯数字正是为了无法扫码时手动输入）；
4. `/m/` 路径与房间 WebSocket（`role=peer`）**均需先登录系统密码**才能加入；
5. 在顶部输入框填写**设备名称**，失焦即广播给其它设备；
6. 每加入一台设备，页面下方就多一张**该设备的卡片**（卡片标题=设备名，卡内左「发送」右「接收」，窄屏自动竖排），可与每台设备分别**双向、多次**互发。

**架构（mesh + 信令 + 中继兜底）**：同一个 `TransferSession` Durable Object 同时服务两种模式，靠连接 tag 区分（方式一 `s`/`r`；方式二主会话 `peer`；方式二配对中继 `s`/`r`）。方式二中每对设备有两级通道：

- **主会话（`role=peer`）**：每设备一条到房间 DO（`idFromName(密码)`）的 WS。DO 分配稳定 `pid`、维护**设备名册**（roster），加入/离开/改名都向全房广播；并中继 WebRTC 信令（`rename` / `signal`，`signal` 定向转发 SDP/ICE 且附 `from`/`to`）。
- **优先 P2P 直传**：每对设备由 `pid` 字典序较小者作 offerer 发起 WebRTC，建好 DataChannel 后**文件端到端直传，不经服务器**。
- **失败回退 CF 中继（`role=relay`）**：若打洞超时（默认 9s）或连接失败，该对设备各自连一条「配对中继 WS」到**独立的 DO 实例** `idFromName(密码_pidA_pidB)`。该实例里只有这一对设备占 `s`/`r` 两端，二进制在 `s↔r` 间转发——**复用方式一的转发逻辑，二进制天然路由到对端，无需路由头**。
- 单条通道（DataChannel 或中继 WS）内的传输协议与方式一一致（`rmeta → rready → 二进制块 → rack(滑窗) → reof → rdone`，每次传输独立 `tid`）；卡片徽标实时显示「已直连 (P2P)」或「已连接 (中继)」。

```
设备A ════ DataChannel 直传 ════ 设备B        （首选；A、B、C… 两两各一条）
  │  信令/名册经房间 DO(role=peer) 的 WS：
  ├─ {type:"signal", to:B, data:{sdp|ice}} → DO → {type:"signal", from:A, data} → B
  └─ {type:"rename", name}                 → DO → {type:"roster", peers:[...]}（广播全房）

  直连失败时，A、B 各连一条 role=relay 的配对 WS（独立 DO 实例 密码_pidA_pidB，仅 s/r 两端）：
设备A ── relay WS ──> [DO: s↔r 转发] <── relay WS ── 设备B    （CF 中继兜底）
```

> 房间设备数量不限（受限于浏览器可同时维持的连接数）。复用同一 DO 类，**无需新增 Durable Object 或重新 `migrations`**。刷新/断线重连凭 `cid`（存 `sessionStorage`）复用同一 `pid`/槽位，名册与既有连接不因此重建身份。

---

## 🛠️ 部署指南

由于 Cloudflare 的底层架构设计，**Durable Objects 的分布式存储分配必须依赖 `migrations`（数据迁移）指令。因此，该项目首次初始化必须使用 Wrangler 命令行工具**。完成首次部署后，后续的所有代码调整和环境变量修改均可在 Cloudflare 网页端 Dashboard 中独立完成。

### 前置条件

**环境准备**: 本地电脑需要配置好 Node.js 环境（建议使用 LTS 版本）。

### 步骤一：准备项目文件

在本地创建空文件夹，并在其中放入以下两个核心文件：

#### 1. `wrangler.toml` (配置文件)

```toml
# 仅第一次部署需要它：把 worker.js 和这个文件放同一目录，跑一次 `npx wrangler deploy`
name = "file-relay"
main = "worker.js"
compatibility_date = "2026-06-05"

# Durable Object 绑定
[[durable_objects.bindings]]
name = "TRANSFER"
class_name = "TransferSession"

# 这条迁移就是"必须用 wrangler 跑一次"的原因；SQLite 类，免费计划也能用
[[migrations]]
tag = "v1"
new_sqlite_classes = ["TransferSession"]

# 说明：PASSWORD 不写在这里。部署后去 dashboard 的
# Settings -> Variables and Secrets 添加 PASSWORD（Secret 类型），
# 或执行一次 `npx wrangler secret put PASSWORD`。

```

#### 2. `worker.js` (应用核心代码)


### 步骤二：本地编译与首次部署

1. 打开终端，切换到该项目文件夹目录下。
2. 执行账号登录授权：
```bash
npx wrangler login

```


*程序会自动唤起浏览器，点击 **Allow** 授权即可。*
3. **执行首次发布**：
```bash
npx wrangler deploy

```


*当终端输出部署成功并提供了一个 `*.workers.dev` 的网址时，说明底层 Durable Object 命名空间已成功激活并完成关联。*

> **🇨🇳 国内网络环境部署提示**：
> 如果在执行 `npx wrangler` 命令时遇到卡顿或连接超时，请在终端中临时注入你的本地代理：
> * **Windows (PowerShell)**: `$env:HTTP_PROXY="http://127.0.0.1:你的代理端口"`; `$env:HTTPS_PROXY="http://127.0.0.1:你的代理端口"`
> * **Mac/Linux**: `export http_proxy=http://127.0.0.1:你的代理端口`; `export https_proxy=http://127.0.0.1:你的代理端口`
> 
> 

### 步骤三：在云端配置登录密码

1. 登录 [Cloudflare 控制台](https://dash.cloudflare.com/)，进入 **Workers & Pages** -> **`file-relay`**。
2. 切换到顶部的 **Settings（设置）** 选项卡，选择左侧的 **Variables & Secrets（变量和机密）**。
3. 在 **Environment Variables** 区域点击 **Add**：
* **Variable name**: `PASSWORD`
* **Value**: 输入你自定义的系统登录密码。
4. 点击 **Deploy（部署）** 保存。

### 步骤四：绑定自定义域名 (强烈推荐)

> **⚠️ 重要提示**：Cloudflare 默认分配的 `*.workers.dev` 域名在中国大陆地区由于 DNS 污染等原因，通常无法正常访问。为了保证稳定使用，**强烈建议绑定一个托管在 Cloudflare 上的自定义域名**。

1. 在当前 `file-relay` Worker 的管理页面中，切换到顶部的 **Settings（设置）** 选项卡。
2. 在左侧菜单中选择 **Domains & Routes（域和路由）**。
3. 点击 **Add（添加）** -> **Custom Domain（自定义域）**。
4. 输入你已经在 Cloudflare 托管的域名或子域名（例如：`relay.yourdomain.com`），点击添加。
5. 等待几十秒 DNS 解析生效后，即可使用该自定义域名并配合 HTTPS 稳定访问你的中继站。

---

## 📝 踩坑与运维重点记录

### 1. 为什么不能直接在 Cloudflare 网页端直接新建此项目？

Cloudflare 网页端的“部署”按钮仅负责无状态代码的上传。由于 Durable Objects 实例属于强状态绑定，必须向服务器集群提交 `migrations` 数据迁移声明，以完成分布式存储节点的物理划片。网页端 UI 暂未开放此接口，因此必须借助 Wrangler 完成首次的存储空间开辟。

### 2. 访问时提示 `crypto.randomUUID is not a function`？

这是浏览器的标准安全限制。Web Crypto API 仅在**安全上下文 (Secure Context)** 中生效。

* **解决办法**：请检查浏览器地址栏，确保使用的是 **`https://`** 协议访问。Cloudflare 默认提供的 `*.workers.dev` 域名或绑定的自定义域名均自带合法的 SSL 证书，请勿使用明文 `http://` 协议访问，否则浏览器将处于保护机制而拒绝生成 UUID 且无法建立 WSS 连接。

---

## 📄 开源许可证

[MIT License]