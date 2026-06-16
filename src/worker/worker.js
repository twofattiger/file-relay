// ============================================================
// Cloudflare Worker 文件实时中继站（Durable Object · 不落盘）
//   Worker            : 鉴权 / 发页面 / 把 WS 升级请求路由到 DO
//   TransferSession DO: 按 transfer id 配对 sender/receiver，并作为 WebRTC 信令通道
//   ENV PASSWORD      : 登录密码（环境变量/Secret）
//
// 连接策略：接收方打开链接 -> 双方交换偏好(prefs) ->
//   两端都勾选"优先 P2P" 才尝试 WebRTC 打洞（成功直传，失败回退 CF 中继）；
//   任一端不勾 -> 直接 CF 中继，完全不走 WebRTC。
// 仅需一次 `npx wrangler deploy`（建立 DO 迁移），之后改代码/环境变量都可在 dashboard 完成。
// ============================================================

import { DurableObject } from "cloudflare:workers";
const WS_CHUNK_SIZE = 512 * 1024;     // 512 KiB（安全低于 CF WebSocket 单消息 1MiB 上限）
const RTC_CHUNK_SIZE = 256 * 1024;    // 256 KiB (WebRTC DataChannel)
const WINDOW_BYTES = 8 * 1024 * 1024; // 8 MiB 滑动窗口
const ACK_EVERY = 4 * 1024 * 1024;    // 4 MiB 确认一次
const SESSION_TTL = 86400;

// STUN 服务器列表(地址发现)。以后改这里即可,无需进函数体。
const STUN_SERVERS = [
  "stun:stun.cloudflare.com:3478",
  "stun:stun.l.google.com:19302",
];

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path.startsWith("/ws/")) {
      const id = path.split("/")[2];
      if (!id) return new Response("bad id", { status: 400 });
      const role = url.searchParams.get("role");
      // 方式1发送方(sender)、方式2房间端(peer)、方式2配对中继(relay) 均需登录；方式1接收方(receiver) 匿名
      if ((role === "sender" || role === "peer" || role === "relay") && !(await isAuthed(request, env))) return new Response("unauthorized", { status: 401 });
      const stub = env.TRANSFER.get(env.TRANSFER.idFromName(id));
      return stub.fetch(request);
    }
    if (path === "/api/login" && request.method === "POST") return handleLogin(request, env);
    if (path === "/api/logout") return new Response(null, { status: 302, headers: { Location: "/", "Set-Cookie": "sess=; Path=/; Max-Age=0" } });
    if (path.startsWith("/r/")) return html(RECEIVER_HTML);            // 方式1：接收（匿名）

    // 以下页面需登录；未登录统一返回登录页，登录成功后 reload 回到当前地址
    const authed = await isAuthed(request, env);
    if (path === "/") return html(authed ? CHOICE_HTML : LOGIN_HTML);  // 登录后展示功能卡片
    if (path === "/send") return html(authed ? SENDER_HTML : LOGIN_HTML);          // 方式1：分享发送
    if (path === "/room") return html(authed ? ROOM_CREATE_HTML : LOGIN_HTML);     // 方式2：创建房间
    if (path.startsWith("/m/")) return html(authed ? ROOM_HTML : LOGIN_HTML);      // 方式2：房间互传
    return new Response("Not found", { status: 404 });
  },
};

// ---------------- Durable Object ----------------
export class TransferSession extends DurableObject {
  constructor(state, env) { super(state, env); this.state = state; this.env = env; }

  // 拒绝/告知错误：用一条独立连接发出 error 后立即关闭
  reject(message) {
    const dup = new WebSocketPair();
    dup[1].accept();
    try { dup[1].send(JSON.stringify({ type: "error", message })); } catch (e) {}
    dup[1].close(1000, "rejected");   // 用正常关闭码，避免客户端 onerror 覆盖掉具体错误文案
    return new Response(null, { status: 101, webSocket: dup[0] });
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") return new Response("expected websocket", { status: 426 });
    const url = new URL(request.url);
    const want = url.searchParams.get("role");

    if (want === "peer") return this.acceptPeer(url);    // 方式2：多端房间主会话（mesh 信令 + 名册）
    if (want === "relay") return this.acceptRelay(url);  // 方式2：某一对设备的中继配对（P2P 失败时兜底）

    // 方式1：固定 sender(s)/receiver(r) 两端，DO 充当中继/信令管道
    const role = want === "sender" ? "sender" : "receiver";
    const tag = role === "sender" ? "s" : "r";
    if (this.state.getWebSockets(tag).length >= 1) return this.reject("该角色已有连接");
    // transfer id 由发送方创建并先占用 s 槽位，再把 /r/{id} 发出。
    // 接收方连入时若没有发送方在线，说明链接无效或发送方已离开，直接拒绝而非凭空建会话。
    if (role === "receiver" && this.state.getWebSockets("s").length === 0) return this.reject("链接无效，或发送方不在线/已离开");

    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    this.state.acceptWebSocket(server, [tag]);
    server.send(JSON.stringify({ type: "role", role }));

    const others = this.state.getWebSockets(tag === "s" ? "r" : "s");
    if (others.length) {
      try { server.send(JSON.stringify({ type: "peer-joined" })); } catch (e) {}
      for (const o of others) { try { o.send(JSON.stringify({ type: "peer-joined" })); } catch (e) {} }
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  // ---- 方式2 多端房间：每个连接分配稳定 pid，维护名册；文件走端到端 WebRTC，不经 DO ----
  acceptPeer(url) {
    const cid = url.searchParams.get("cid") || "";
    // 同一客户端(cid)的旧连接（刷新/断线重连）：复用其 pid/name，标记 replaced 后关闭
    let pid = null, name = "";
    if (cid) {
      for (const old of this.state.getWebSockets("cid:" + cid)) {
        const a = old.deserializeAttachment() || {};
        if (a.pid) pid = a.pid;
        if (a.name) name = a.name;
        try { old.serializeAttachment({ ...a, replaced: true }); } catch (e) {}
        try { old.close(1000, "replaced"); } catch (e) {}
      }
    }
    if (!pid) pid = crypto.randomUUID();

    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    try { server.serializeAttachment({ pid, name, cid }); } catch (e) {}
    this.state.acceptWebSocket(server, ["peer", "pid:" + pid, "cid:" + cid]);
    try { server.send(JSON.stringify({ type: "self", pid })); } catch (e) {}
    this.broadcastRoster();
    return new Response(null, { status: 101, webSocket: client });
  }

  // ---- 方式2 中继配对：独立 DO 实例（idFromName(房间_pidA_pidB)），仅一对设备占 s/r 两端。
  //      文件二进制在 s↔r 之间转发（复用 webSocketMessage 的方式1分支），无需路由头。 ----
  acceptRelay(url) {
    const cid = url.searchParams.get("cid") || "";
    let reclaimed = null;   // 同一端刷新/重连：复用其槽位
    if (cid) {
      for (const old of this.state.getWebSockets("cid:" + cid)) {
        const t = this.state.getTags(old);
        const slot = t.includes("s") ? "s" : (t.includes("r") ? "r" : null);
        if (slot) reclaimed = slot;
        try { old.serializeAttachment({ replaced: true }); } catch (e) {}
        try { old.close(1000, "replaced"); } catch (e) {}
      }
    }
    let tag;
    if (reclaimed) tag = reclaimed;
    else if (this.state.getWebSockets("s").length === 0) tag = "s";
    else if (this.state.getWebSockets("r").length === 0) tag = "r";
    else return this.reject("中继配对已满");

    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    try { server.serializeAttachment({ cid }); } catch (e) {}
    this.state.acceptWebSocket(server, [tag, "cid:" + cid]);

    const others = this.state.getWebSockets(tag === "s" ? "r" : "s");
    if (others.length) {   // 两端齐 → 双方就绪
      try { server.send(JSON.stringify({ type: "peer-joined" })); } catch (e) {}
      for (const o of others) { try { o.send(JSON.stringify({ type: "peer-joined" })); } catch (e) {} }
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  // 当前在线设备名册（排除被替换的旧连接，可额外排除一个正在关闭的连接）
  roster(exclude) {
    const peers = [];
    for (const ws of this.state.getWebSockets("peer")) {
      if (ws === exclude || this.isReplaced(ws)) continue;
      const a = ws.deserializeAttachment() || {};
      if (a.pid) peers.push({ pid: a.pid, name: a.name || "" });
    }
    return peers;
  }
  broadcastRoster(exclude) {
    const msg = JSON.stringify({ type: "roster", peers: this.roster(exclude) });
    for (const ws of this.state.getWebSockets("peer")) {
      if (ws === exclude || this.isReplaced(ws)) continue;
      try { ws.send(msg); } catch (e) {}
    }
  }

  // 被新连接替换的旧连接：不向对端发离线通知（否则刷新重连会让对端误判“离开”）
  isReplaced(ws) { try { const a = ws.deserializeAttachment(); return !!(a && a.replaced); } catch (e) { return false; } }

  async webSocketMessage(ws, message) {
    if (this.state.getTags(ws).includes("peer")) return this.peerMessage(ws, message);
    // 方式1：prefs / webrtc-* / meta / 二进制等所有消息一律原样转发给对端
    const isSender = this.state.getTags(ws).includes("s");
    const targets = this.state.getWebSockets(isSender ? "r" : "s");
    if (!targets.length) { try { ws.send(JSON.stringify({ type: "peer-closed" })); } catch (e) {} return; }
    try { targets[0].send(message); } catch (e) { try { ws.send(JSON.stringify({ type: "error", message: "relay failed" })); } catch (_) {} }
  }

  // 方式2：只中继 JSON 信令（rename 改名 / signal 定向 WebRTC 信令）；文件二进制走 P2P 不到这里
  peerMessage(ws, message) {
    if (typeof message !== "string") return;
    let m; try { m = JSON.parse(message); } catch (e) { return; }
    const a = ws.deserializeAttachment() || {};
    if (m.type === "rename") {
      a.name = String(m.name || "").slice(0, 40);
      try { ws.serializeAttachment(a); } catch (e) {}
      this.broadcastRoster();
    } else if (m.type === "signal" && m.to) {
      const out = JSON.stringify({ type: "signal", from: a.pid, data: m.data });
      for (const t of this.state.getWebSockets("pid:" + m.to)) {
        if (this.isReplaced(t)) continue;
        try { t.send(out); } catch (e) {}
      }
    }
  }

  async webSocketClose(ws) {
    if (this.isReplaced(ws)) { try { ws.close(1000); } catch (e) {} return; }
    if (this.state.getTags(ws).includes("peer")) { try { ws.close(1000); } catch (e) {} this.broadcastRoster(ws); return; }
    const isSender = this.state.getTags(ws).includes("s");
    for (const o of this.state.getWebSockets(isSender ? "r" : "s")) { try { o.send(JSON.stringify({ type: "peer-closed" })); } catch (e) {} }
    try { ws.close(1000); } catch (e) {}
  }

  async webSocketError(ws) {
    try {
      if (this.isReplaced(ws)) return;
      if (this.state.getTags(ws).includes("peer")) { this.broadcastRoster(ws); return; }
      const isSender = this.state.getTags(ws).includes("s");
      for (const o of this.state.getWebSockets(isSender ? "r" : "s")) { try { o.send(JSON.stringify({ type: "peer-closed" })); } catch (e) {} }
    } catch (e) {}
  }
}

// ---------------- 鉴权（环境变量密码 + 无状态签名 Cookie） ----------------
async function handleLogin(request, env) {
  if (!env.PASSWORD) return json({ ok: false, error: "未配置 PASSWORD 环境变量" }, 500);
  const b = await safeJson(request);
  if ((b.password || "") !== env.PASSWORD) return json({ ok: false, error: "密码错误" }, 401);
  const token = await makeToken(env.PASSWORD);
  const secure = new URL(request.url).protocol === "https:" ? "; Secure" : "";
  const cookie = "sess=" + token + "; HttpOnly" + secure + "; SameSite=Lax; Path=/; Max-Age=" + SESSION_TTL;
  return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { "Content-Type": "application/json", "Set-Cookie": cookie } });
}
async function isAuthed(request, env) { if (!env.PASSWORD) return false; return verifyToken(env.PASSWORD, parseCookies(request)["sess"]); }
async function makeToken(secret) { const exp = Date.now() + SESSION_TTL * 1000; return exp + "." + (await hmac(secret, String(exp))); }
async function verifyToken(secret, token) {
  if (!token) return false;
  const i = token.indexOf("."); if (i < 0) return false;
  const exp = token.slice(0, i), sig = token.slice(i + 1);
  if (!/^\d+$/.test(exp) || Number(exp) < Date.now()) return false;
  return timingEq(sig, await hmac(secret, exp));
}
async function hmac(secret, msg) {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey("raw", enc.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(msg));
  return b64url(new Uint8Array(sig));
}
function b64url(bytes) { let s = ""; for (const b of bytes) s += String.fromCharCode(b); return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, ""); }
function timingEq(a, b) { if (a.length !== b.length) return false; let r = 0; for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i); return r === 0; }

// ---------------- 通用 ----------------
function html(body) { return new Response(body, { headers: { "Content-Type": "text/html; charset=utf-8" } }); }
function json(obj, status) { return new Response(JSON.stringify(obj), { status: status || 200, headers: { "Content-Type": "application/json" } }); }
async function safeJson(request) { try { return await request.json(); } catch (e) { return {}; } }
function parseCookies(request) {
  const out = {}; const raw = request.headers.get("Cookie") || "";
  for (const part of raw.split(";")) { const i = part.indexOf("="); if (i > -1) out[part.slice(0, i).trim()] = part.slice(i + 1).trim(); }
  return out;
}

// ============================================================
// 页面（内嵌 JS 用普通字符串拼接，避免与外层模板的 ${}/反引号冲突）
// ============================================================
const STYLE =
  "<style>" +
  "*{box-sizing:border-box}body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#0f1115;color:#e6e6e6;display:flex;min-height:100vh;align-items:center;justify-content:center;padding:20px}" +
  ".card{width:100%;max-width:520px;background:#171a21;border:1px solid #262b36;border-radius:14px;padding:28px}" +
  "h1{font-size:18px;margin:0 0 18px;display:flex;align-items:center;flex-wrap:wrap;gap:8px}input,button{font-size:16px}" +
  "input[type=password],input[type=text]{width:100%;padding:11px 12px;background:#0f1115;border:1px solid #2c3340;border-radius:9px;color:#e6e6e6;margin-bottom:12px}" +
  "input[type=file]{margin-bottom:14px}" +
  "button{cursor:pointer;padding:11px 16px;border:0;border-radius:9px;background:#3b82f6;color:#fff;font-weight:600}" +
  "button:disabled{opacity:.4;cursor:not-allowed}button.ghost{background:#262b36}" +
  ".row{display:flex;gap:8px}.row input{flex:1;margin-bottom:0}" +
  "#status{margin-top:16px;font-size:14px;color:#9aa4b2;min-height:56px;white-space:pre-line;line-height:1.7}" +
  ".barwrap{height:8px;background:#0f1115;border-radius:6px;overflow:hidden;margin-top:14px;border:1px solid #2c3340}" +
  "#bar,#sbar,#rbar,.pbar{height:100%;width:0;background:linear-gradient(90deg,#3b82f6,#22d3ee);transition:width .15s}" +
  ".hint{font-size:12px;color:#6b7280;margin-top:10px}" +
  ".info{background:#0f1115;border:1px solid #2c3340;border-radius:9px;padding:12px;margin-bottom:14px;font-size:14px;display:none}" +
  ".opt{display:flex;align-items:center;gap:8px;margin-bottom:14px;font-size:13px;color:#9aa4b2;cursor:pointer;user-select:none}" +
  ".opt input{width:16px;height:16px;margin:0;accent-color:#3b82f6;cursor:pointer;flex:none}" +
  ".opt input:disabled{cursor:not-allowed;opacity:.5}" +
  ".badge{display:none;font-size:12px;font-weight:600;padding:3px 10px;border-radius:999px;background:#262b36;color:#9aa4b2;border:1px solid #2c3340}" +
  ".badge.p2p{background:#064e3b;color:#34d399;border-color:#065f46}" +
  ".badge.relay{background:#1e293b;color:#60a5fa;border-color:#1e40af}" +
  // 功能卡片选择页
  ".card.wide{max-width:840px}" +
  ".choice{display:flex;gap:16px;flex-wrap:wrap}" +
  ".tile{flex:1;min-width:200px;background:#0f1115;border:1px solid #2c3340;border-radius:12px;padding:24px 18px;text-align:center;cursor:pointer;text-decoration:none;color:#e6e6e6;transition:border-color .15s,transform .1s}" +
  ".tile:hover{border-color:#3b82f6;transform:translateY(-2px)}" +
  ".tile .ic{font-size:34px}.tile .tt{font-weight:600;margin-top:10px;font-size:16px}" +
  ".tile .ds{font-size:12px;color:#9aa4b2;margin-top:6px;line-height:1.5}" +
  "h1 a.back{margin-left:auto;font-size:13px;color:#9aa4b2;text-decoration:none;font-weight:400}" +
  // 房间互传：顶部信息 + 左右两框
  ".roomtop{background:#0f1115;border:1px solid #2c3340;border-radius:12px;padding:16px;margin-bottom:6px}" +
  ".panes{display:flex;gap:16px;flex-wrap:wrap;margin-top:18px}" +
  ".pane{flex:1;min-width:260px;background:#0f1115;border:1px solid #2c3340;border-radius:12px;padding:16px}" +
  ".pane h2{font-size:15px;margin:0 0 12px}.pane h3{font-size:13px;margin:0 0 10px;color:#cbd5e1}" +
  ".substat{margin-top:10px;font-size:13px;color:#9aa4b2;min-height:20px;white-space:pre-line;line-height:1.6}" +
  ".recvitem{background:#171a21;border:1px solid #2c3340;border-radius:9px;padding:12px;margin-bottom:10px;font-size:13px;display:flex;flex-direction:column;gap:10px}" +
  // 多端设备互传：设备名输入框 + 每设备一张卡片（卡内左发右收）
  "#rname{flex:none;width:130px;margin:0 4px;padding:7px 10px;font-size:13px}" +
  ".device{background:#14171e;border:1px solid #262b36;border-radius:12px;padding:14px;margin-top:14px}" +
  ".device .panes{margin-top:0}" +
  ".dev-head{display:flex;align-items:center;gap:10px;margin-bottom:6px}" +
  ".dev-name{font-weight:600;font-size:15px}" +
  ".dev-conn{margin-left:auto;font-size:12px;padding:3px 10px;border-radius:999px;background:#262b36;color:#9aa4b2;border:1px solid #2c3340}" +
  ".dev-conn.ok{background:#064e3b;color:#34d399;border-color:#065f46}" +
  ".dev-conn.bad{background:#3f1d1d;color:#fca5a5;border-color:#7f1d1d}" +
  ".empty{color:#6b7280;font-size:13px;text-align:center;padding:22px 10px}" +
  "@media (max-width:480px){.card{padding:20px}body{padding:10px}}" +
  "</style>";

const LOGIN_HTML =
  "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'>" +
  "<title>中继站 · 登录</title>" + STYLE +
  "<div class=card><h1>🔐 输入系统密码</h1>" +
  "<input id=pw type=password placeholder='密码' autofocus>" +
  "<button id=go>进入</button><div id=status></div></div>" +
  "<script>" +
  "var pw=document.getElementById('pw'),go=document.getElementById('go'),st=document.getElementById('status');" +
  "function login(){go.disabled=true;st.textContent='验证中...';" +
  "fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pw.value})})" +
  ".then(function(r){return r.json();}).then(function(d){if(d.ok){location.reload();}else{st.textContent=d.error||'失败';go.disabled=false;}})" +
  ".catch(function(){st.textContent='网络错误';go.disabled=false;});}" +
  "go.onclick=login;pw.addEventListener('keydown',function(e){if(e.key==='Enter')login();});" +
  "</script>";

// 登录后的功能选择页：两个卡片 —— 1. 分享发送(方式1)  2. 设备互传(方式2)
const CHOICE_HTML =
  "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'>" +
  "<title>中继站 · 功能</title>" + STYLE +
  "<div class=card><h1>🚀 选择功能</h1>" +
  "<div class=choice>" +
  "<a class=tile href='/send'><div class=ic>📤</div><div class=tt>分享发送</div><div class=ds>选择文件，生成链接 / 二维码，发给对方一次性接收</div></a>" +
  "<a class=tile href='/room'><div class=ic>🔁</div><div class=tt>设备互传</div><div class=ds>创建房间，多台设备登录后两两直连、双向多次互发文件</div></a>" +
  "</div>" +
  "<div style='margin-top:18px;text-align:right'><a href='/api/logout' style='font-size:13px;color:#9aa4b2;text-decoration:none'>退出登录</a></div>" +
  "</div>";

// 方式2 · 创建房间页：设置房间密码（默认当前分秒），生成后进入 /m/{密码}
const ROOM_CREATE_HTML =
  "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'>" +
  "<title>中继站 · 设备互传</title>" + STYLE +
  "<div class=card><h1>🔁 设备互传 · 创建房间 <a class=back href='/'>返回</a></h1>" +
  "<div class=hint style='margin-bottom:14px'>设置一个房间密码（默认取当前时间的分秒，方便另一台设备手动输入加入）。链接形如 域名/m/密码。</div>" +
  "<input id=pass type=text inputmode=numeric maxlength=8 placeholder='房间密码'>" +
  "<button id=create>生成房间</button></div>" +
  "<script>" +
  "var passEl=document.getElementById('pass'),createEl=document.getElementById('create');" +
  "var d=new Date();passEl.value=String(d.getMinutes()).padStart(2,'0')+String(d.getSeconds()).padStart(2,'0');" +
  "function go(){var v=(passEl.value||'').trim();if(!v)return;location.href='/m/'+encodeURIComponent(v);}" +
  "createEl.onclick=go;passEl.addEventListener('keydown',function(e){if(e.key==='Enter')go();});" +
  "passEl.focus();passEl.select();" +
  "</script>";

// 方式2 · 房间互传页：顶部二维码/链接，下方左右两框（发送 / 接收），双向、可多次
const ROOM_HTML =
  "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'>" +
  "<title>中继站 · 设备互传</title>" + STYLE +
  "<script src='https://cdnjs.cloudflare.com/ajax/libs/qrious/4.0.2/qrious.min.js'></script>" +
  "<div class='card wide'><h1>🔁 设备互传 <input id=rname type=text placeholder='设备名称' maxlength=20><a class=back href='/'>返回</a></h1>" +
  "<div class=roomtop>" +
  "<div class=row><input id=rlink type=text readonly><button id=rcopy class=ghost>复制</button></div>" +
  "<div style='text-align:center;margin-top:12px'><canvas id=qr style='background:#fff;padding:8px;border-radius:8px'></canvas></div>" +
  "<div class=hint>让其它设备扫码，或在浏览器直接输入此链接（域名/m/密码）加入；对方同样需先登录系统密码。每加入一台设备会在下方出现一张卡片，可与其双向互发文件。</div></div>" +
  "<div id=devices></div>" +
  "<div id=status></div></div>" +
  "<script>" + ROOM_JS() + "</script>";

const SENDER_HTML =
  "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'>" +
  "<title>中继站 · 发送</title>" + STYLE +
  "<script src='https://cdnjs.cloudflare.com/ajax/libs/qrious/4.0.2/qrious.min.js'></script>" +
  "<div class=card><h1>📤 实时发送文件 <span id=badge class=badge></span><a class=back href='/'>返回</a></h1>" +
  "<input id=file type=file>" +
  "<label class=opt><input id=p2p type=checkbox checked> ⚡ 优先 P2P 直连（尝试点对点直连，否则通过服务端中继）</label>" +
  "<button id=gen disabled>生成传输链接</button>" +
  "<div id=linkbox style='display:none;margin-top:14px'>" +
  "<div class=row><input id=link type=text readonly><button id=copy class=ghost>复制</button></div>" +
  "<div style='text-align:center;margin-top:14px'><canvas id=qr style='background:#fff;padding:8px;border-radius:8px'></canvas></div>" +
  "<div class=hint>把链接发给对方，或让对方直接扫描上方二维码，对方打开并选择保存位置后开始实时传输（双方需保持页面打开）</div></div>" +
  "<div class=barwrap><div id=bar></div></div>" +
  "<div id=status></div></div>" +
  "<script>" + SENDER_JS() + "</script>";

const RECEIVER_HTML =
  "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'>" +
  "<title>中继站 · 接收</title>" + STYLE +
  "<div class=card><h1>📥 接收文件 <span id=badge class=badge></span></h1>" +
  "<div id=info class=info></div>" +
  "<label class=opt><input id=p2p type=checkbox checked> ⚡ 优先 P2P 直连（尝试点对点直连，否则通过服务端中继）</label>" +
  "<button id=save disabled>选择保存位置并接收</button>" +
  "<div class=barwrap><div id=bar></div></div>" +
  "<div id=status>连接中...</div></div>" +
  "<script>" + RECEIVER_JS() + "</script>";

function FMT_JS() {
  return "function fmt(b){if(b<1024)return b+' B';var u=['KB','MB','GB','TB'],i=-1;do{b=b/1024;i++;}while(b>=1024&&i<u.length-1);return b.toFixed(1)+' '+u[i];}" +
    "function fmtTime(s){s=Math.round(s);if(s<60)return s+'s';var m=Math.floor(s/60),r=s%60;if(m<60)return m+'m'+(r?r+'s':'');var h=Math.floor(m/60);m=m%60;return h+'h'+(m?m+'m':'');}";
}

function SENDER_JS() {
  return FMT_JS() + `
var WS_CHUNK = ${WS_CHUNK_SIZE}, RTC_CHUNK = ${RTC_CHUNK_SIZE}, WINDOW = ${WINDOW_BYTES};
var ICE = ${JSON.stringify(STUN_SERVERS.map(function(u){return {urls:u};}))};
var CHUNK = WS_CHUNK;
var ws = null, rtc = null, dc = null, transport = 'ws';
var file = null, id = null, t0 = 0, done = false;
var sent = 0, acked = 0, offset = 0, eofSent = false, metaSent = false, started = false, pumping = false, rerun = false;
var rtcTimeout = null, remoteSet = false, pendingIce = [];
var peerP2P = true, prefsTimer = null, decided = false, negotiating = false; // 对端默认想要 P2P；prefs 到达后修正
var fileEl = document.getElementById('file'), genEl = document.getElementById('gen'), p2pEl = document.getElementById('p2p');
var st = document.getElementById('status'), bar = document.getElementById('bar'), badge = document.getElementById('badge');
var linkbox = document.getElementById('linkbox'), linkEl = document.getElementById('link');

function S(s) { st.textContent = s; }
function setBadge(t) {
  if (!badge) return;
  if (t === 'webrtc') { badge.textContent = 'P2P 直连'; badge.className = 'badge p2p'; }
  else if (t === 'ws') { badge.textContent = 'CF 中继'; badge.className = 'badge relay'; }
  else { badge.textContent = '协商中'; badge.className = 'badge'; }
  badge.style.display = 'inline-block';
}

fileEl.onchange = function() { file = fileEl.files[0] || null; genEl.disabled = !file; };
document.getElementById('copy').onclick = function() { linkEl.select(); if (navigator.clipboard) navigator.clipboard.writeText(linkEl.value); };
genEl.onclick = function() {
  if (!file) return;
  id = crypto.randomUUID();
  linkEl.value = location.origin + '/r/' + id;
  linkbox.style.display = 'block';
  genEl.disabled = true; fileEl.disabled = true;
  
  try {
    new QRious({
      element: document.getElementById('qr'),
      value: linkEl.value,
      size: 160
    });
  } catch(e) { console.error('qr fail', e); }

  connect();
};

function reset() {
  if (done) return;
  offset = 0; sent = 0; acked = 0; eofSent = false; metaSent = false; started = false; bar.style.width = '0%';
  transport = 'ws'; CHUNK = WS_CHUNK; remoteSet = false; pendingIce = [];
  decided = false; negotiating = false; peerP2P = true;
  clearTimeout(rtcTimeout); clearTimeout(prefsTimer);
  if (rtc) { try { rtc.close(); } catch (e) {} rtc = null; dc = null; }
  setBadge('pending');
}

function connect() {
  var p = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(p + '://' + location.host + '/ws/' + id + '?role=sender');
  ws.binaryType = 'arraybuffer';
  ws.onopen = function() { S('已连接中继，等待对方打开链接...'); setBadge('pending'); };
  ws.onerror = function() { S('连接错误'); };
  ws.onmessage = function(ev) {
    if (typeof ev.data !== 'string') return;
    var m = JSON.parse(ev.data);
    if (m.type === 'peer-joined') {
      if (!metaSent && !started && !negotiating) {
        negotiating = true;
        prefsTimer = setTimeout(decideTransport, 3000); // 收不到对端偏好则按当前值决定
      }
    } else if (m.type === 'prefs') {
      peerP2P = !!m.p2p;
      decideTransport();
    } else if (m.type === 'webrtc-answer') {
      if (rtc) rtc.setRemoteDescription(new RTCSessionDescription(m.sdp)).then(function() { remoteSet = true; flushIce(); }).catch(function(e) { console.error(e); fallbackWS(); });
    } else if (m.type === 'webrtc-ice') {
      addIce(m.candidate);
    } else if (m.type === 'ready') {
      started = true; t0 = Date.now(); S(transport === 'webrtc' ? '正在直传...' : '正在中继传输...'); pump();
    } else if (m.type === 'ack') {
      acked = m.bytes; pump();
    } else if (m.type === 'complete') {
      done = true; bar.style.width = '100%'; S('传输完成 ✓'); cleanup();
    } else if (m.type === 'peer-closed') {
      if (!done) { S('对方已断开，等待重新连接...'); reset(); }
    } else if (m.type === 'error') {
      S('错误: ' + (m.message || ''));
    }
  };
}

// 两端都勾选"优先 P2P" 才尝试打洞；否则直接走 CF 中继，不发起 WebRTC
function decideTransport() {
  if (metaSent || started || decided) return;
  decided = true;
  clearTimeout(prefsTimer);
  p2pEl.disabled = true;                  // 锁定复选框，传输中不可改
  var tryP2P = p2pEl.checked && peerP2P;
  if (tryP2P) {
    initWebRTC();
  } else {
    S('通过 CF 中继传输...');
    fallbackWS();
  }
}

function addIce(c) {
  if (!rtc) return;
  if (remoteSet) rtc.addIceCandidate(new RTCIceCandidate(c)).catch(function() {});
  else pendingIce.push(c);
}
function flushIce() {
  var arr = pendingIce; pendingIce = [];
  for (var i = 0; i < arr.length; i++) { try { rtc.addIceCandidate(new RTCIceCandidate(arr[i])).catch(function() {}); } catch (e) {} }
}

function initWebRTC() {
  S('尝试建立点对点直连(WebRTC)...'); setBadge('pending');
  try {
    remoteSet = false; pendingIce = [];
    rtc = new RTCPeerConnection({ iceServers: ICE });
    dc = rtc.createDataChannel('file-transfer', { ordered: true });
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = WINDOW / 2;
    dc.onbufferedamountlow = function() { if (started) pump(); };
    dc.onopen = function() {
      if (metaSent) return;
      clearTimeout(rtcTimeout);
      transport = 'webrtc'; CHUNK = RTC_CHUNK; setBadge('webrtc');
      S('点对点直连成功，等待对方确认接收...');
      sendMeta();
    };
    dc.onmessage = function(ev) {
      if (typeof ev.data !== 'string') return;
      var m = JSON.parse(ev.data);
      if (m.type === 'ready') { started = true; t0 = Date.now(); S('正在直传...'); pump(); }
      else if (m.type === 'ack') { acked = m.bytes; pump(); }
      else if (m.type === 'complete') { done = true; bar.style.width = '100%'; S('传输完成 ✓'); cleanup(); }
    };
    rtc.onicecandidate = function(ev) {
      if (ev.candidate && ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'webrtc-ice', candidate: ev.candidate }));
    };
    rtc.onconnectionstatechange = function() {
      if (!rtc) return;
      var s = rtc.connectionState;
      if (s === 'failed' || s === 'closed') {
        if (!metaSent) { S('直连失败，回退到中继传输...'); fallbackWS(); }
        else if (!done) { S('直连中断'); }
      }
    };
    rtc.createOffer().then(function(offer) { return rtc.setLocalDescription(offer); })
      .then(function() { ws.send(JSON.stringify({ type: 'webrtc-offer', sdp: rtc.localDescription })); })
      .catch(function(e) { console.error('offer error', e); fallbackWS(); });
    rtcTimeout = setTimeout(function() {
      if (!metaSent) { S('直连超时，回退到中继传输...'); fallbackWS(); }
    }, 6000);
  } catch (e) { console.error('webrtc init error', e); fallbackWS(); }
}

function fallbackWS() {
  if (metaSent) return;
  clearTimeout(rtcTimeout);
  if (rtc) { try { rtc.close(); } catch (e) {} rtc = null; dc = null; }
  transport = 'ws'; CHUNK = WS_CHUNK; setBadge('ws');
  sendMeta();
}

function sendMsg(data) {
  if (transport === 'webrtc' && dc && dc.readyState === 'open') dc.send(data);
  else if (ws && ws.readyState === 1) ws.send(data);
}

function sendMeta() {
  if (metaSent) return;
  metaSent = true;
  if (transport === 'ws') S('对方已连接，等待对方确认接收(中继模式)...');
  sendMsg(JSON.stringify({ type: 'meta', name: file.name, size: file.size, mime: file.type || 'application/octet-stream', chunkSize: CHUNK }));
}

async function pump() {
  if (pumping) { rerun = true; return; }
  pumping = true;
  do {
    rerun = false;
    while (offset < file.size && (sent - acked) < WINDOW && (transport !== 'webrtc' || !dc || dc.bufferedAmount < WINDOW)) {
      var end = Math.min(offset + CHUNK, file.size);
      var buf = await file.slice(offset, end).arrayBuffer();
      sendMsg(buf);
      sent += buf.byteLength;
      offset = end;
      prog();
    }
  } while (rerun);
  pumping = false;
  if (offset >= file.size && !eofSent) { sendMsg(JSON.stringify({ type: 'eof' })); eofSent = true; }
}

function prog() {
  var pct = file.size ? Math.floor(sent * 100 / file.size) : 0;
  bar.style.width = pct + '%';
  var sec = (Date.now() - t0) / 1000, sp = sec > 0 ? sent / sec : 0;
  var eta = (sp > 0 && sent < file.size) ? '  剩余约 ' + fmtTime((file.size - sent) / sp) : '';
  S((transport === 'webrtc' ? '直传中 ' : '中继中 ') + pct + '%\\n' + fmt(sent) + ' / ' + fmt(file.size) + '  (' + fmt(sp) + '/s)\\n已耗时 ' + fmtTime(sec) + eta);
}

function cleanup() { try { if (ws) ws.close(); } catch (e) {} try { if (rtc) rtc.close(); } catch (e) {} }
`;
}

function RECEIVER_JS() {
  return FMT_JS() + `
var ACKEVERY = ${ACK_EVERY};
var ICE = ${JSON.stringify(STUN_SERVERS.map(function(u){return {urls:u};}))};
var id = location.pathname.split('/')[2];
var ws = null, rtc = null, dc = null, transport = 'ws';
var meta = null, writable = null, useMem = false, chunks = [], writeChain = Promise.resolve();
var received = 0, written = 0, lastAck = 0, t0 = 0, finalizing = false;
var remoteSet = false, pendingIce = [];
var prefsSent = false;
var info = document.getElementById('info'), save = document.getElementById('save'), p2pEl = document.getElementById('p2p');
var st = document.getElementById('status'), bar = document.getElementById('bar'), badge = document.getElementById('badge');

function S(s) { st.textContent = s; }
function setBadge(t) {
  if (!badge) return;
  if (t === 'webrtc') { badge.textContent = 'P2P 直连'; badge.className = 'badge p2p'; }
  else if (t === 'ws') { badge.textContent = 'CF 中继'; badge.className = 'badge relay'; }
  else { badge.textContent = '协商中'; badge.className = 'badge'; }
  badge.style.display = 'inline-block';
}

function connect() {
  var p = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(p + '://' + location.host + '/ws/' + id + '?role=receiver');
  ws.binaryType = 'arraybuffer';
  ws.onopen = function() { S('已连接，等待发送方...'); };
  ws.onerror = function() { S('连接错误'); };
  ws.onmessage = function(ev) {
    if (typeof ev.data === 'string') {
      var m = JSON.parse(ev.data);
      if (m.type === 'peer-joined') {
        S('发送方在线，正在协商连接...'); setBadge('pending');
        if (!prefsSent) {                 // 把本端"是否优先 P2P"告知发送方，并锁定复选框
          prefsSent = true;
          p2pEl.disabled = true;
          if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'prefs', p2p: p2pEl.checked }));
        }
      }
      else if (m.type === 'webrtc-offer') {
        if (!p2pEl.checked) return;       // 本端不勾优先 P2P：拒收 offer，等待中继 meta（双保险）
        handleWebRTCOffer(m.sdp);
      }
      else if (m.type === 'webrtc-ice') { addIce(m.candidate); }
      else handleMessage(m, 'ws');
    } else {
      onChunk(ev.data);
    }
  };
}

function addIce(c) {
  if (!rtc) return;
  if (remoteSet) rtc.addIceCandidate(new RTCIceCandidate(c)).catch(function() {});
  else pendingIce.push(c);
}
function flushIce() {
  var arr = pendingIce; pendingIce = [];
  for (var i = 0; i < arr.length; i++) { try { rtc.addIceCandidate(new RTCIceCandidate(arr[i])).catch(function() {}); } catch (e) {} }
}

function handleWebRTCOffer(sdp) {
  try {
    remoteSet = false; pendingIce = [];
    rtc = new RTCPeerConnection({ iceServers: ICE });
    rtc.onicecandidate = function(ev) {
      if (ev.candidate && ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'webrtc-ice', candidate: ev.candidate }));
    };
    rtc.ondatachannel = function(ev) {
      dc = ev.channel;
      dc.binaryType = 'arraybuffer';
      dc.onmessage = function(e) {
        if (typeof e.data === 'string') handleMessage(JSON.parse(e.data), 'webrtc');
        else onChunk(e.data);
      };
    };
    rtc.setRemoteDescription(new RTCSessionDescription(sdp))
      .then(function() { remoteSet = true; flushIce(); return rtc.createAnswer(); })
      .then(function(answer) { return rtc.setLocalDescription(answer); })
      .then(function() { ws.send(JSON.stringify({ type: 'webrtc-answer', sdp: rtc.localDescription })); })
      .catch(function(e) { console.error('answer error', e); });
  } catch (e) { console.error('webrtc answer error', e); }
}

function handleMessage(m, source) {
  if (m.type === 'meta') {
    transport = source; setBadge(source);
    meta = m;
    info.style.display = 'block';
    info.textContent = '文件: ' + m.name + '  (' + fmt(m.size) + ')';
    save.disabled = false;
    S('点击下方按钮选择保存位置 [' + (source === 'webrtc' ? 'P2P 直连' : 'CF 中继') + ']');
  }
  else if (m.type === 'eof') { finalize(); }
  else if (m.type === 'peer-closed') { if (!finalizing) S('发送方已断开'); }
  else if (m.type === 'error') { S('错误: ' + (m.message || '')); }
}

function sendMsg(data) {
  if (transport === 'webrtc' && dc && dc.readyState === 'open') dc.send(data);
  else if (ws && ws.readyState === 1) ws.send(data);
}

save.onclick = async function() {
  if (!meta) return;
  if (window.showSaveFilePicker) {
    try {
      var h = await showSaveFilePicker({ suggestedName: meta.name });
      writable = await h.createWritable();
    } catch (e) {
      if (e && e.name === 'AbortError') return;
      useMem = true;
    }
  } else {
    useMem = true;
  }
  save.disabled = true; t0 = Date.now();
  sendMsg(JSON.stringify({ type: 'ready' }));
  S(useMem ? '内存接收中(大文件慎用)...' : (transport === 'webrtc' ? '开始直连接收...' : '开始中继接收...'));
};

async function onChunk(buf) {
  received += buf.byteLength;
  var p = writeChain.then(function() {
    if (writable) return writable.write(buf);
    chunks.push(buf);
  });
  writeChain = p;
  await p;
  written += buf.byteLength;
  prog();
  if (written - lastAck >= ACKEVERY) {
    lastAck = written;
    sendMsg(JSON.stringify({ type: 'ack', bytes: written }));
  }
}

function prog() {
  var pct = meta && meta.size ? Math.floor(received * 100 / meta.size) : 0;
  bar.style.width = pct + '%';
  var sec = (Date.now() - t0) / 1000, sp = sec > 0 ? received / sec : 0;
  var eta = (sp > 0 && meta && received < meta.size) ? '  剩余约 ' + fmtTime((meta.size - received) / sp) : '';
  S((transport === 'webrtc' ? '直连接收 ' : '中继接收 ') + pct + '%\\n' + fmt(received) + ' / ' + fmt(meta ? meta.size : 0) + '  (' + fmt(sp) + '/s)\\n已耗时 ' + fmtTime(sec) + eta);
}

async function finalize() {
  if (finalizing) return;
  finalizing = true;
  await writeChain;
  sendMsg(JSON.stringify({ type: 'ack', bytes: written }));
  if (writable) {
    await writable.close();
  } else {
    var blob = new Blob(chunks, { type: meta.mime });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url; a.download = meta.name;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(function() { URL.revokeObjectURL(url); }, 10000);
  }
  sendMsg(JSON.stringify({ type: 'complete' }));
  bar.style.width = '100%';
  S('接收完成 ✓  ' + meta.name);
  try { if (ws) ws.close(); if (rtc) rtc.close(); } catch (e) {}
}

connect();
`;
}

// ============================================================
// 方式2 · 房间互传客户端：两端对等，各自维护一个「发送上下文」与一个「接收上下文」。
//   发送串行（一次一个文件，传完才能发下一个），接收串行；双向同时进行互不干扰，
//   因为每端只处理「自己收到的二进制块」(对端→本端) 与「自己发出的块」(本端→对端)。
//   传输走 WebSocket 经 DO 中继，消息均带 tid 以区分不同次传输。
// ============================================================
// 方式2 · 多设备房间客户端（WebRTC mesh）：
//   信令/名册经 DO 的 WS；文件在设备间用各自的 DataChannel 端到端直传，不经服务器。
//   每个远程设备 = 一个 Peer 对象 + 一张卡片（卡内左「发送」右「接收」，窄屏自动竖排）。
//   每对设备由 pid 字典序较小的一方作为 offerer，避免双方同时发起(glare)。
// ============================================================
// 方式2 · 多设备房间客户端（WebRTC mesh，失败回退 CF 中继）：
//   主会话(role=peer)经 DO 的 WS 收发：设备名册(roster) + WebRTC 信令(signal)。
//   每个远程设备 = 一个 Peer 对象 + 一张卡片（卡内左「发送」右「接收」，窄屏自动竖排）。
//   每对设备优先用 WebRTC DataChannel 端到端直传(transport='webrtc')；打洞失败/超时则
//   各自连一条 role=relay 的「配对中继 WS」回退(transport='relay')——该配对是独立 DO 实例
//   (idFromName(房间_pidA_pidB))，里面只有 s/r 两端，二进制天然路由到对端。
//   每对由 pid 字典序较小者作 offerer，避免 glare。发送/接收逻辑与通道无关(pSend 统一)。
// ============================================================
function ROOM_JS() {
  return FMT_JS() + `
var WS_CHUNK = ${WS_CHUNK_SIZE}, RTC_CHUNK = ${RTC_CHUNK_SIZE}, WINDOW = ${WINDOW_BYTES}, ACKEVERY = ${ACK_EVERY};
var ICE = ${JSON.stringify(STUN_SERVERS.map(function(u){return {urls:u};}))};
var P2P_TIMEOUT = 9000;   // 直连协商超时(ms)，超时回退中继
var pass = location.pathname.split('/')[2] || '';
var id = pass, cid = roomCid();
var ws = null, myPid = null, peers = {};   // pid -> Peer
var myName = loadName();

function uuid() { return crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + Math.random().toString(16).slice(2); }
function roomCid() { var k = 'room-cid-' + pass; try { var v = sessionStorage.getItem(k); if (!v) { v = uuid(); sessionStorage.setItem(k, v); } return v; } catch (e) { return uuid(); } }
function loadName() { try { return localStorage.getItem('room-name') || ''; } catch (e) { return ''; } }
function saveName(n) { try { localStorage.setItem('room-name', n); } catch (e) {} }

var nameEl = document.getElementById('rname');
var devicesEl = document.getElementById('devices');
var linkEl = document.getElementById('rlink');
var stat = document.getElementById('status');

function S(s) { stat.textContent = s; }
function escapeHtml(s) { return String(s).replace(/[&<>"']/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }

// 顶部：链接 / 复制 / 二维码 / 设备名
linkEl.value = location.origin + '/m/' + pass;
document.getElementById('rcopy').onclick = function() { linkEl.select(); if (navigator.clipboard) navigator.clipboard.writeText(linkEl.value); };
try { new QRious({ element: document.getElementById('qr'), value: linkEl.value, size: 150 }); } catch (e) { console.error('qr fail', e); }
nameEl.value = myName;
nameEl.addEventListener('change', commitName);
nameEl.addEventListener('blur', commitName);
function commitName() {                       // 失焦/回车提交，变化才广播
  var n = (nameEl.value || '').trim().slice(0, 20);
  if (n === myName) return;
  myName = n; saveName(n);
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'rename', name: myName }));
}

renderEmpty();
connect();

function connect() {
  var p = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(p + '://' + location.host + '/ws/' + id + '?role=peer&cid=' + encodeURIComponent(cid));
  ws.binaryType = 'arraybuffer';
  ws.onopen = function() { S('已进入房间「' + pass + '」，等待其它设备加入…'); };
  ws.onerror = function() { S('连接错误'); };
  ws.onclose = function() { S('与服务器断开，请刷新重连'); };
  ws.onmessage = function(ev) {
    if (typeof ev.data !== 'string') return;   // 主会话只走信令，不传文件
    var m = JSON.parse(ev.data);
    if (m.type === 'self') { myPid = m.pid; if (myName) ws.send(JSON.stringify({ type: 'rename', name: myName })); }
    else if (m.type === 'roster') updateRoster(m.peers);
    else if (m.type === 'signal') handleSignal(m.from, m.data);
  };
}

/* ---------------- 设备名册 ---------------- */
function updateRoster(list) {
  var seen = {};
  for (var i = 0; i < list.length; i++) {
    var p = list[i];
    if (p.pid && p.pid !== myPid) { seen[p.pid] = 1; ensurePeer(p.pid, p.name); }
  }
  Object.keys(peers).forEach(function(pid) { if (!seen[pid]) removePeer(pid); });
  renderEmpty();
}
function renderEmpty() {
  var has = Object.keys(peers).length > 0;
  var e = document.getElementById('empty');
  if (has) { if (e) e.remove(); }
  else if (!e) {
    var d = document.createElement('div'); d.id = 'empty'; d.className = 'empty';
    d.textContent = '还没有其它设备。让另一台设备扫码或访问相同链接加入。';
    devicesEl.appendChild(d);
  }
}
function amOfferer(pid) { return String(myPid) < String(pid); }  // pid 较小者发起，保证每对唯一 offerer

function ensurePeer(pid, name) {
  var pr = peers[pid];
  if (!pr) {
    pr = peers[pid] = { pid: pid, name: name || '', transport: null, chunk: WS_CHUNK,
      pc: null, dc: null, remoteSet: false, pendingIce: [],
      relayWs: null, ready: false, p2pTimer: null, reconnectT: null,
      sendCtx: null, recvCtx: null, els: null };
    buildCard(pr);
    devicesEl.appendChild(pr.els.root);
  }
  if (name != null) { pr.name = name; setName(pr); }
  if (!pr.pc && !pr.relayWs) {        // 尚未建立任何通道 → 开始协商（优先 P2P）
    armP2PTimeout(pr);
    if (amOfferer(pid)) startConnect(pr);
  }
}
function removePeer(pid) {
  var pr = peers[pid]; if (!pr) return;
  teardownPC(pr); teardownRelay(pr); clearTimeout(pr.p2pTimer);
  if (pr.els && pr.els.root) pr.els.root.remove();
  delete peers[pid];
}

/* ---------------- 通道协商：优先 WebRTC，失败回退 CF 中继 ---------------- */
function armP2PTimeout(pr) {
  if (pr.p2pTimer) return;
  pr.p2pTimer = setTimeout(function() { if (!pr.ready && peers[pr.pid]) fallbackRelay(pr); }, P2P_TIMEOUT);
}
function signal(pid, data) { if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'signal', to: pid, data: data })); }
function createPC(pr) {
  teardownPC(pr);
  pr.remoteSet = false; pr.pendingIce = [];
  pr.pc = new RTCPeerConnection({ iceServers: ICE });
  pr.pc.onicecandidate = function(ev) { if (ev.candidate) signal(pr.pid, { ice: ev.candidate }); };
  pr.pc.onconnectionstatechange = function() { onConnState(pr); };
  pr.pc.ondatachannel = function(ev) { attachDC(pr, ev.channel); };
}
function startConnect(pr) {                   // offerer
  createPC(pr);
  attachDC(pr, pr.pc.createDataChannel('file', { ordered: true }));
  setConn(pr, '直连协商中…', '');
  pr.pc.createOffer().then(function(o) { return pr.pc.setLocalDescription(o); })
    .then(function() { signal(pr.pid, { sdp: pr.pc.localDescription }); })
    .catch(function(e) { console.error(e); fallbackRelay(pr); });
}
function handleSignal(from, data) {
  if (from === myPid) return;
  var pr = peers[from];
  if (!pr) { ensurePeer(from, null); pr = peers[from]; }
  if (!pr) return;
  if (data.sdp) {
    if (data.sdp.type === 'offer') {
      createPC(pr);                            // answerer：新建 PC 接收（丢弃旧的）
      setConn(pr, '直连协商中…', '');
      pr.pc.setRemoteDescription(new RTCSessionDescription(data.sdp))
        .then(function() { pr.remoteSet = true; flushIce(pr); return pr.pc.createAnswer(); })
        .then(function(a) { return pr.pc.setLocalDescription(a); })
        .then(function() { signal(pr.pid, { sdp: pr.pc.localDescription }); })
        .catch(function(e) { console.error(e); fallbackRelay(pr); });
    } else if (pr.pc) {                         // answer：offerer 收到
      pr.pc.setRemoteDescription(new RTCSessionDescription(data.sdp))
        .then(function() { pr.remoteSet = true; flushIce(pr); })
        .catch(function(e) { console.error(e); });
    }
  } else if (data.ice) {
    if (pr.pc && pr.remoteSet) pr.pc.addIceCandidate(new RTCIceCandidate(data.ice)).catch(function() {});
    else if (pr.pc) pr.pendingIce.push(data.ice);
  }
}
function flushIce(pr) {
  var arr = pr.pendingIce; pr.pendingIce = [];
  for (var i = 0; i < arr.length; i++) { try { pr.pc.addIceCandidate(new RTCIceCandidate(arr[i])).catch(function() {}); } catch (e) {} }
}
function attachDC(pr, ch) {
  pr.dc = ch; ch.binaryType = 'arraybuffer';
  ch.bufferedAmountLowThreshold = WINDOW / 2;
  ch.onbufferedamountlow = function() { if (pr.sendCtx) pumpSend(pr); };
  ch.onopen = function() {                      // 直连成功：关掉中继兜底，切到 P2P
    clearTimeout(pr.p2pTimer); pr.p2pTimer = null;
    teardownRelay(pr);
    pr.transport = 'webrtc'; pr.chunk = RTC_CHUNK; pr.ready = true;
    setConn(pr, '已直连 (P2P)', 'ok'); refreshSendBtn(pr);
  };
  ch.onclose = function() { if (pr.transport === 'webrtc') { pr.ready = false; refreshSendBtn(pr); } };
  ch.onmessage = function(ev) { if (typeof ev.data === 'string') onCtrl(pr, JSON.parse(ev.data)); else onChunk(pr, ev.data); };
}
function onConnState(pr) {
  if (!pr.pc) return;
  var s = pr.pc.connectionState;
  if (s === 'connecting') { if (!pr.ready) setConn(pr, '直连协商中…', ''); }
  else if (s === 'disconnected') { if (pr.transport === 'webrtc') setConn(pr, '网络波动…', 'bad'); }  // 可能自行恢复
  else if (s === 'failed' || s === 'closed') {
    if (!peers[pr.pid]) return;
    abortPeerTransfers(pr);
    fallbackRelay(pr);                          // 直连不行 → 回退 CF 中继
  }
}
function teardownPC(pr) {
  clearTimeout(pr.reconnectT);
  if (pr.dc) { try { pr.dc.close(); } catch (e) {} pr.dc = null; }
  if (pr.pc) { try { pr.pc.close(); } catch (e) {} pr.pc = null; }
  pr.remoteSet = false; pr.pendingIce = [];
  if (pr.transport === 'webrtc') { pr.transport = null; pr.ready = false; }
}

// 回退到 CF 中继：拆掉 P2P，连一条该对设备专用的 relay 配对 WS
function fallbackRelay(pr) {
  if (!peers[pr.pid]) return;
  if (pr.transport === 'relay' && pr.ready) return;   // 已在中继且可用
  clearTimeout(pr.p2pTimer); pr.p2pTimer = null;
  teardownPC(pr);
  connectRelay(pr);
}
function relayId(pr) { return pass + '_' + [String(myPid), String(pr.pid)].sort().join('_'); }
function connectRelay(pr) {
  if (pr.relayWs) return;
  setConn(pr, '中继连接中…', '');
  var p = location.protocol === 'https:' ? 'wss' : 'ws';
  var w = pr.relayWs = new WebSocket(p + '://' + location.host + '/ws/' + encodeURIComponent(relayId(pr)) + '?role=relay&cid=' + encodeURIComponent(cid));
  w.binaryType = 'arraybuffer';
  w.onerror = function() {};
  w.onclose = function() { if (pr.relayWs === w) { pr.relayWs = null; if (pr.transport === 'relay') { pr.ready = false; refreshSendBtn(pr); } } };
  w.onmessage = function(ev) {
    if (typeof ev.data !== 'string') { onChunk(pr, ev.data); return; }
    var m = JSON.parse(ev.data);
    if (m.type === 'peer-joined') { pr.transport = 'relay'; pr.chunk = WS_CHUNK; pr.ready = true; setConn(pr, '已连接 (中继)', 'ok'); refreshSendBtn(pr); }
    else if (m.type === 'peer-closed') { pr.ready = false; refreshSendBtn(pr); abortPeerTransfers(pr); setConn(pr, '对方已断开', 'bad'); }
    else if (m.type === 'error') { setConn(pr, '中继失败：' + (m.message || ''), 'bad'); }
    else onCtrl(pr, m);
  };
}
function teardownRelay(pr) {
  if (pr.relayWs) { try { pr.relayWs.onclose = null; pr.relayWs.close(); } catch (e) {} pr.relayWs = null; }
  if (pr.transport === 'relay') { pr.transport = null; pr.ready = false; }
}

/* ---------------- 设备卡片 UI ---------------- */
function buildCard(pr) {
  var root = document.createElement('div'); root.className = 'device';
  var head = document.createElement('div'); head.className = 'dev-head';
  var nm = document.createElement('span'); nm.className = 'dev-name';
  var cn = document.createElement('span'); cn.className = 'dev-conn';
  head.appendChild(nm); head.appendChild(cn);
  var panes = document.createElement('div'); panes.className = 'panes';
  // 发送
  var sp = document.createElement('div'); sp.className = 'pane'; sp.innerHTML = '<h3>📤 发送</h3>';
  var file = document.createElement('input'); file.type = 'file';
  var btn = document.createElement('button'); btn.textContent = '发送'; btn.disabled = true;
  var sw = document.createElement('div'); sw.className = 'barwrap'; var sb = document.createElement('div'); sb.className = 'pbar'; sw.appendChild(sb);
  var ss = document.createElement('div'); ss.className = 'substat';
  sp.appendChild(file); sp.appendChild(btn); sp.appendChild(sw); sp.appendChild(ss);
  // 接收
  var rp = document.createElement('div'); rp.className = 'pane'; rp.innerHTML = '<h3>📥 接收</h3>';
  var rl = document.createElement('div');
  var rw = document.createElement('div'); rw.className = 'barwrap'; var rb = document.createElement('div'); rb.className = 'pbar'; rw.appendChild(rb);
  var rs = document.createElement('div'); rs.className = 'substat';
  rp.appendChild(rl); rp.appendChild(rw); rp.appendChild(rs);
  panes.appendChild(sp); panes.appendChild(rp);
  root.appendChild(head); root.appendChild(panes);
  pr.els = { root: root, name: nm, conn: cn, file: file, btn: btn, sbar: sb, sstat: ss, rlist: rl, rbar: rb, rstat: rs };
  file.onchange = function() { refreshSendBtn(pr); };
  btn.onclick = function() { startSend(pr); };
  setName(pr); setConn(pr, '连接中…', '');
}
function setName(pr) { if (pr.els) pr.els.name.textContent = pr.name || '未命名设备'; }
function setConn(pr, text, cls) { if (pr.els) { pr.els.conn.textContent = text; pr.els.conn.className = 'dev-conn' + (cls ? ' ' + cls : ''); } }
function refreshSendBtn(pr) { if (pr.els) pr.els.btn.disabled = !(pr.els.file.files.length && pr.ready && !pr.sendCtx); }

/* ---------------- 发送（每设备；通道无关：webrtc=DataChannel / relay=配对 WS） ---------------- */
function chOpen(pr) { return pr.transport === 'relay' ? (pr.relayWs && pr.relayWs.readyState === 1) : (pr.dc && pr.dc.readyState === 'open'); }
function chBuffered(pr) { var b = pr.transport === 'relay' ? (pr.relayWs && pr.relayWs.bufferedAmount) : (pr.dc && pr.dc.bufferedAmount); return b || 0; }
function pSend(pr, data) { if (pr.transport === 'relay') { if (pr.relayWs && pr.relayWs.readyState === 1) pr.relayWs.send(data); } else if (pr.dc && pr.dc.readyState === 'open') pr.dc.send(data); }
function pctrl(pr, obj) { pSend(pr, JSON.stringify(obj)); }

function startSend(pr) {
  var f = pr.els.file.files[0];
  if (!f || pr.sendCtx || !pr.ready) return;
  pr.sendCtx = { file: f, tid: uuid(), sent: 0, acked: 0, offset: 0, eof: false, t0: 0, pumping: false, rerun: false };
  pr.els.btn.disabled = true; pr.els.file.disabled = true; pr.els.sbar.style.width = '0%';
  pr.els.sstat.textContent = '已请求发送 ' + f.name + '，等待对方确认接收…';
  pctrl(pr, { type: 'rmeta', tid: pr.sendCtx.tid, name: f.name, size: f.size, mime: f.type || 'application/octet-stream' });
}
function onReady(pr, m) { var c = pr.sendCtx; if (!c || m.tid !== c.tid) return; c.t0 = Date.now(); pr.els.sstat.textContent = '开始发送…'; pumpSend(pr); }
function onAck(pr, m) { var c = pr.sendCtx; if (c && m.tid === c.tid) { c.acked = m.bytes; pumpSend(pr); } }
async function pumpSend(pr) {
  var c = pr.sendCtx; if (!c) return;
  if (c.pumping) { c.rerun = true; return; }
  c.pumping = true;
  do {
    c.rerun = false;
    while (c.offset < c.file.size && (c.sent - c.acked) < WINDOW && chBuffered(pr) < WINDOW) {
      var end = Math.min(c.offset + pr.chunk, c.file.size);
      var buf = await c.file.slice(c.offset, end).arrayBuffer();
      if (pr.sendCtx !== c || !chOpen(pr)) { c.pumping = false; return; }
      pSend(pr, buf); c.sent += buf.byteLength; c.offset = end; sendProg(pr);
    }
  } while (c.rerun);
  c.pumping = false;
  if (c.offset >= c.file.size && !c.eof) { c.eof = true; pctrl(pr, { type: 'reof', tid: c.tid }); }
}
function sendProg(pr) {
  var c = pr.sendCtx; if (!c) return;
  var pct = c.file.size ? Math.floor(c.sent * 100 / c.file.size) : 0;
  pr.els.sbar.style.width = pct + '%';
  var sec = (Date.now() - c.t0) / 1000, sp = sec > 0 ? c.sent / sec : 0;
  pr.els.sstat.textContent = '发送中 ' + pct + '%  ' + fmt(c.sent) + ' / ' + fmt(c.file.size) + '  (' + fmt(sp) + '/s)';
}
function onSendDone(pr, m) {
  var c = pr.sendCtx; if (!c || m.tid !== c.tid) return;
  pr.els.sbar.style.width = '100%';
  pr.els.sstat.textContent = '已发送完成 ✓  ' + c.file.name;
  pr.sendCtx = null; pr.els.file.disabled = false; pr.els.file.value = ''; refreshSendBtn(pr);
}

/* ---------------- 接收（每设备） ---------------- */
function onCtrl(pr, m) {
  switch (m.type) {
    case 'rmeta': startRecv(pr, m); break;
    case 'rready': onReady(pr, m); break;
    case 'rack': onAck(pr, m); break;
    case 'reof': onRecvEof(pr, m); break;
    case 'rdone': onSendDone(pr, m); break;
  }
}
function startRecv(pr, m) {
  if (pr.recvCtx) return;       // 串行：对端一次发一个
  pr.recvCtx = { tid: m.tid, meta: m, writable: null, useMem: false, chunks: [], writeChain: Promise.resolve(), received: 0, written: 0, lastAck: 0, t0: 0, finalizing: false };
  pr.els.rbar.style.width = '0%'; pr.els.rlist.innerHTML = '';
  var box = document.createElement('div'); box.className = 'recvitem';
  var info = document.createElement('div'); info.innerHTML = '收到文件：' + escapeHtml(m.name) + '  (' + fmt(m.size) + ')';
  var b = document.createElement('button'); b.className = 'ghost'; b.textContent = '保存接收';
  b.onclick = function() { acceptRecv(pr, b); };
  box.appendChild(info); box.appendChild(b); pr.els.rlist.appendChild(box);
  pr.els.rstat.textContent = '对方请求发送文件，点击「保存接收」开始';
}
async function acceptRecv(pr, b) {
  var c = pr.recvCtx; if (!c) return;
  b.disabled = true;
  if (window.showSaveFilePicker) {
    try { var h = await showSaveFilePicker({ suggestedName: c.meta.name }); c.writable = await h.createWritable(); }
    catch (e) { if (e && e.name === 'AbortError') { b.disabled = false; return; } c.useMem = true; }
  } else { c.useMem = true; }
  c.t0 = Date.now();
  pctrl(pr, { type: 'rready', tid: c.tid });
  pr.els.rstat.textContent = c.useMem ? '内存接收中(大文件慎用)…' : '接收中…';
}
async function onChunk(pr, buf) {
  var c = pr.recvCtx; if (!c) return;
  c.received += buf.byteLength;
  var p = c.writeChain.then(function() { if (c.writable) return c.writable.write(buf); c.chunks.push(buf); });
  c.writeChain = p; await p;
  c.written += buf.byteLength; recvProg(pr);
  if (c.written - c.lastAck >= ACKEVERY) { c.lastAck = c.written; pctrl(pr, { type: 'rack', tid: c.tid, bytes: c.written }); }
}
function recvProg(pr) {
  var c = pr.recvCtx; if (!c) return;
  var pct = c.meta.size ? Math.floor(c.received * 100 / c.meta.size) : 0;
  pr.els.rbar.style.width = pct + '%';
  var sec = (Date.now() - c.t0) / 1000, sp = sec > 0 ? c.received / sec : 0;
  pr.els.rstat.textContent = '接收中 ' + pct + '%  ' + fmt(c.received) + ' / ' + fmt(c.meta.size) + '  (' + fmt(sp) + '/s)';
}
async function onRecvEof(pr, m) {
  var c = pr.recvCtx; if (!c || m.tid !== c.tid || c.finalizing) return;
  c.finalizing = true; await c.writeChain;
  pctrl(pr, { type: 'rack', tid: c.tid, bytes: c.written });
  if (c.writable) { await c.writable.close(); }
  else {
    var blob = new Blob(c.chunks, { type: c.meta.mime });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a'); a.href = url; a.download = c.meta.name;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(function() { URL.revokeObjectURL(url); }, 10000);
  }
  pctrl(pr, { type: 'rdone', tid: c.tid });
  pr.els.rbar.style.width = '100%';
  pr.els.rstat.textContent = '接收完成 ✓  ' + c.meta.name;
  pr.els.rlist.innerHTML = ''; pr.recvCtx = null;
}

// 与某设备连接中断：放弃与它进行中的收发，恢复其卡片
function abortPeerTransfers(pr) {
  if (pr.sendCtx) { pr.sendCtx = null; if (pr.els) { pr.els.file.disabled = false; pr.els.sbar.style.width = '0%'; pr.els.sstat.textContent = '连接中断，发送已停止'; } }
  if (pr.recvCtx) { try { if (pr.recvCtx.writable) pr.recvCtx.writable.abort(); } catch (e) {} pr.recvCtx = null; if (pr.els) { pr.els.rlist.innerHTML = ''; pr.els.rbar.style.width = '0%'; pr.els.rstat.textContent = '连接中断，接收已停止'; } }
  if (pr.els) refreshSendBtn(pr);
}
`;
}