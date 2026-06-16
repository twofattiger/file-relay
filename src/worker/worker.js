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
      // 方式1发送方(sender) 与 方式2房间端(peer) 均需登录；方式1接收方(receiver) 匿名
      if ((role === "sender" || role === "peer") && !(await isAuthed(request, env))) return new Response("unauthorized", { status: 401 });
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

  // 取连接占用的槽位（s/r）
  slotTag(ws) { const t = this.state.getTags(ws); return t.includes("s") ? "s" : (t.includes("r") ? "r" : null); }

  // 拒绝/告知错误：用一条独立连接发出 error 后立即关闭
  reject(message) {
    const dup = new WebSocketPair();
    dup[1].accept();
    try { dup[1].send(JSON.stringify({ type: "error", message })); } catch (e) {}
    dup[1].close(1008, "rejected");
    return new Response(null, { status: 101, webSocket: dup[0] });
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") return new Response("expected websocket", { status: 426 });
    const url = new URL(request.url);
    const want = url.searchParams.get("role");

    let role, tag, extraTags = [];
    if (want === "peer") {
      // 方式2房间：对等端。先回收“同一客户端(cid)的旧连接”（刷新/断线重连），复用其槽位，
      // 避免把刚刷新的自己当成第三台设备而拒绝。旧连接打上 replaced 标记后再关闭，
      // 这样它的 webSocketClose 不会向对端误发 peer-closed。
      const cid = url.searchParams.get("cid") || "";
      let reclaimed = null;
      if (cid) {
        for (const old of this.state.getWebSockets("cid:" + cid)) {
          reclaimed = this.slotTag(old) || reclaimed;
          try { old.serializeAttachment({ replaced: true }); } catch (e) {}
          try { old.close(1000, "replaced"); } catch (e) {}
        }
      }
      if (reclaimed) tag = reclaimed;
      else if (this.state.getWebSockets("s").length === 0) tag = "s";
      else if (this.state.getWebSockets("r").length === 0) tag = "r";
      else return this.reject("房间已满（最多 2 台设备）");
      role = tag === "s" ? "sender" : "receiver";
      if (cid) extraTags = ["cid:" + cid];
    } else {
      role = want === "sender" ? "sender" : "receiver";
      tag = role === "sender" ? "s" : "r";
      if (this.state.getWebSockets(tag).length >= 1) return this.reject("该角色已有连接");
    }

    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    this.state.acceptWebSocket(server, [tag, ...extraTags]);
    server.send(JSON.stringify({ type: "role", role }));

    const opp = tag === "s" ? "r" : "s";
    const others = this.state.getWebSockets(opp);
    if (others.length) {
      try { server.send(JSON.stringify({ type: "peer-joined" })); } catch (e) {}
      for (const o of others) { try { o.send(JSON.stringify({ type: "peer-joined" })); } catch (e) {} }
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  // 被新连接替换的旧连接：不向对端发 peer-closed（否则刷新重连会让对端误判“对方离开”）
  isReplaced(ws) { try { const a = ws.deserializeAttachment(); return !!(a && a.replaced); } catch (e) { return false; } }

  // prefs / webrtc-* / meta / 二进制等所有消息一律原样转发给对端
  async webSocketMessage(ws, message) {
    const isSender = this.state.getTags(ws).includes("s");
    const targets = this.state.getWebSockets(isSender ? "r" : "s");
    if (!targets.length) { try { ws.send(JSON.stringify({ type: "peer-closed" })); } catch (e) {} return; }
    try { targets[0].send(message); } catch (e) { try { ws.send(JSON.stringify({ type: "error", message: "relay failed" })); } catch (_) {} }
  }

  async webSocketClose(ws) {
    if (this.isReplaced(ws)) { try { ws.close(1000); } catch (e) {} return; }
    const isSender = this.state.getTags(ws).includes("s");
    for (const o of this.state.getWebSockets(isSender ? "r" : "s")) { try { o.send(JSON.stringify({ type: "peer-closed" })); } catch (e) {} }
    try { ws.close(1000); } catch (e) {}
  }

  async webSocketError(ws) {
    try {
      if (this.isReplaced(ws)) return;
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
  "h1{font-size:18px;margin:0 0 18px;display:flex;align-items:center;gap:8px}input,button{font-size:16px}" +
  "input[type=password],input[type=text]{width:100%;padding:11px 12px;background:#0f1115;border:1px solid #2c3340;border-radius:9px;color:#e6e6e6;margin-bottom:12px}" +
  "input[type=file]{margin-bottom:14px}" +
  "button{cursor:pointer;padding:11px 16px;border:0;border-radius:9px;background:#3b82f6;color:#fff;font-weight:600}" +
  "button:disabled{opacity:.4;cursor:not-allowed}button.ghost{background:#262b36}" +
  ".row{display:flex;gap:8px}.row input{flex:1;margin-bottom:0}" +
  "#status{margin-top:16px;font-size:14px;color:#9aa4b2;min-height:56px;white-space:pre-line;line-height:1.7}" +
  ".barwrap{height:8px;background:#0f1115;border-radius:6px;overflow:hidden;margin-top:14px;border:1px solid #2c3340}" +
  "#bar,#sbar,#rbar{height:100%;width:0;background:linear-gradient(90deg,#3b82f6,#22d3ee);transition:width .15s}" +
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
  ".pane h2{font-size:15px;margin:0 0 12px}" +
  ".substat{margin-top:10px;font-size:13px;color:#9aa4b2;min-height:20px;white-space:pre-line;line-height:1.6}" +
  ".recvitem{background:#171a21;border:1px solid #2c3340;border-radius:9px;padding:12px;margin-bottom:10px;font-size:13px;display:flex;flex-direction:column;gap:10px}" +
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
  "<a class=tile href='/room'><div class=ic>🔁</div><div class=tt>设备互传</div><div class=ds>创建房间，两台设备登录后左右双向、多次互发文件</div></a>" +
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
  "<div class='card wide'><h1>🔁 设备互传 <span id=badge class=badge></span><a class=back href='/'>返回</a></h1>" +
  "<div class=roomtop>" +
  "<div class=row><input id=rlink type=text readonly><button id=rcopy class=ghost>复制</button></div>" +
  "<div style='text-align:center;margin-top:12px'><canvas id=qr style='background:#fff;padding:8px;border-radius:8px'></canvas></div>" +
  "<div class=hint>让另一台设备扫码，或在浏览器直接输入此链接（域名/m/密码）加入；对方同样需先登录系统密码。</div></div>" +
  "<div class=panes>" +
  "<div class=pane><h2>📤 发送给对方</h2>" +
  "<input id=rfile type=file>" +
  "<button id=rsend disabled>发送</button>" +
  "<div class=barwrap><div id=sbar></div></div>" +
  "<div id=sstat class=substat></div></div>" +
  "<div class=pane><h2>📥 接收对方文件</h2>" +
  "<div id=rlist></div>" +
  "<div class=barwrap><div id=rbar></div></div>" +
  "<div id=rstat class=substat></div></div>" +
  "</div>" +
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
function ROOM_JS() {
  return FMT_JS() + `
var WS_CHUNK = ${WS_CHUNK_SIZE}, WINDOW = ${WINDOW_BYTES}, ACKEVERY = ${ACK_EVERY};
var pass = location.pathname.split('/')[2] || '';
var id = pass;                       // 房间 id 即密码（DO idFromName）
var cid = roomCid();                 // 本端稳定标识：刷新/重连复用同一槽位，不被当作第三台设备
var ws = null, peerOnline = false;

// 客户端 id 存 sessionStorage：同标签刷新保留(=重连)，关闭标签即弃
function roomCid() {
  var k = 'room-cid-' + pass;
  try {
    var v = sessionStorage.getItem(k);
    if (!v) { v = (crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + Math.random().toString(16).slice(2)); sessionStorage.setItem(k, v); }
    return v;
  } catch (e) { return String(Date.now()) + Math.random().toString(16).slice(2); }
}
var sendCtx = null, recvCtx = null;  // 同一时刻各最多一个

var fileEl = document.getElementById('rfile'), sendBtn = document.getElementById('rsend');
var sbar = document.getElementById('sbar'), sstat = document.getElementById('sstat');
var rlist = document.getElementById('rlist'), rbar = document.getElementById('rbar'), rstat = document.getElementById('rstat');
var linkEl = document.getElementById('rlink');
var stat = document.getElementById('status'), badge = document.getElementById('badge');

function S(s) { stat.textContent = s; }
function setBadge(on) { badge.textContent = on ? '对方在线' : '等待对方'; badge.className = 'badge ' + (on ? 'p2p' : ''); badge.style.display = 'inline-block'; }
function escapeHtml(s) { return String(s).replace(/[&<>"']/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
function wsSend(d) { if (ws && ws.readyState === 1) ws.send(d); }
function refreshSendBtn() { sendBtn.disabled = !(fileEl.files.length && peerOnline) || !!sendCtx; }

// 顶部：链接 / 复制 / 二维码
linkEl.value = location.origin + '/m/' + pass;
document.getElementById('rcopy').onclick = function() { linkEl.select(); if (navigator.clipboard) navigator.clipboard.writeText(linkEl.value); };
try { new QRious({ element: document.getElementById('qr'), value: linkEl.value, size: 150 }); } catch (e) { console.error('qr fail', e); }
setBadge(false);
connect();

function connect() {
  var p = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(p + '://' + location.host + '/ws/' + id + '?role=peer&cid=' + encodeURIComponent(cid));
  ws.binaryType = 'arraybuffer';
  ws.onopen = function() { S('已进入房间「' + pass + '」，等待对方加入...'); };
  ws.onerror = function() { S('连接错误'); };
  ws.onclose = function() { peerOnline = false; setBadge(false); refreshSendBtn(); };
  ws.onmessage = function(ev) {
    if (typeof ev.data === 'string') route(JSON.parse(ev.data));
    else onRecvChunk(ev.data);
  };
}

function route(m) {
  switch (m.type) {
    case 'peer-joined': peerOnline = true; setBadge(true); S('对方已加入，可互相发送文件'); refreshSendBtn(); break;
    case 'peer-closed': peerOnline = false; setBadge(false); abortTransfers(); S('对方已离开，等待重新加入...'); break;
    case 'rmeta': startRecv(m); break;     // 对端要发文件
    case 'rready': onReady(m); break;       // 对端已就绪可开始接收
    case 'rack': onAck(m); break;           // 接收方流控确认
    case 'reof': onRecvEof(m); break;       // 对端发送完毕
    case 'rdone': onSendDone(m); break;     // 接收方已落盘完成
    case 'error': S('错误: ' + (m.message || '')); break;
  }
}

// 对端离开：放弃进行中的收发，恢复界面，便于对方重连后重试
function abortTransfers() {
  if (sendCtx) { sendCtx = null; fileEl.disabled = false; sbar.style.width = '0%'; sstat.textContent = '对方离开，发送已中断'; }
  if (recvCtx) { try { if (recvCtx.writable) recvCtx.writable.abort(); } catch (e) {} recvCtx = null; rlist.innerHTML = ''; rbar.style.width = '0%'; rstat.textContent = '对方离开，接收已中断'; }
  refreshSendBtn();
}

/* ---------------- 发送侧 ---------------- */
fileEl.onchange = refreshSendBtn;
sendBtn.onclick = function() {
  var f = fileEl.files[0];
  if (!f || sendCtx || !peerOnline) return;
  sendCtx = { file: f, tid: crypto.randomUUID(), sent: 0, acked: 0, offset: 0, eof: false, t0: 0, pumping: false, rerun: false };
  sendBtn.disabled = true; fileEl.disabled = true;
  sbar.style.width = '0%';
  sstat.textContent = '已请求发送 ' + f.name + '，等待对方确认接收...';
  wsSend(JSON.stringify({ type: 'rmeta', tid: sendCtx.tid, name: f.name, size: f.size, mime: f.type || 'application/octet-stream' }));
};

function onReady(m) {
  if (!sendCtx || m.tid !== sendCtx.tid) return;
  sendCtx.t0 = Date.now();
  sstat.textContent = '开始发送...';
  pumpSend();
}
function onAck(m) { if (sendCtx && m.tid === sendCtx.tid) { sendCtx.acked = m.bytes; pumpSend(); } }

async function pumpSend() {
  var c = sendCtx;
  if (!c) return;
  if (c.pumping) { c.rerun = true; return; }
  c.pumping = true;
  do {
    c.rerun = false;
    while (c.offset < c.file.size && (c.sent - c.acked) < WINDOW) {
      var end = Math.min(c.offset + WS_CHUNK, c.file.size);
      var buf = await c.file.slice(c.offset, end).arrayBuffer();
      if (!sendCtx || sendCtx.tid !== c.tid) { c.pumping = false; return; } // 期间被重置
      wsSend(buf);
      c.sent += buf.byteLength; c.offset = end;
      sendProg();
    }
  } while (c.rerun);
  c.pumping = false;
  if (c.offset >= c.file.size && !c.eof) { c.eof = true; wsSend(JSON.stringify({ type: 'reof', tid: c.tid })); }
}

function sendProg() {
  var c = sendCtx; if (!c) return;
  var pct = c.file.size ? Math.floor(c.sent * 100 / c.file.size) : 0;
  sbar.style.width = pct + '%';
  var sec = (Date.now() - c.t0) / 1000, sp = sec > 0 ? c.sent / sec : 0;
  sstat.textContent = '发送中 ' + pct + '%  ' + fmt(c.sent) + ' / ' + fmt(c.file.size) + '  (' + fmt(sp) + '/s)';
}

function onSendDone(m) {
  if (!sendCtx || m.tid !== sendCtx.tid) return;
  var name = sendCtx.file.name;
  sbar.style.width = '100%';
  sstat.textContent = '已发送完成 ✓  ' + name;
  sendCtx = null;
  fileEl.disabled = false; fileEl.value = '';
  refreshSendBtn();
}

/* ---------------- 接收侧 ---------------- */
function startRecv(m) {
  if (recvCtx) return;   // 串行：对端一次只发一个，忽略意外并发
  recvCtx = { tid: m.tid, meta: m, writable: null, useMem: false, chunks: [], writeChain: Promise.resolve(), received: 0, written: 0, lastAck: 0, t0: 0, finalizing: false };
  rbar.style.width = '0%';
  rlist.innerHTML = '';
  var box = document.createElement('div'); box.className = 'recvitem';
  var info = document.createElement('div'); info.innerHTML = '收到文件：' + escapeHtml(m.name) + '  (' + fmt(m.size) + ')';
  var btn = document.createElement('button'); btn.className = 'ghost'; btn.textContent = '保存接收';
  btn.onclick = function() { acceptRecv(btn); };
  box.appendChild(info); box.appendChild(btn); rlist.appendChild(box);
  rstat.textContent = '对方请求发送文件，点击「保存接收」开始';
}

async function acceptRecv(btn) {
  var c = recvCtx; if (!c) return;
  btn.disabled = true;
  if (window.showSaveFilePicker) {
    try { var h = await showSaveFilePicker({ suggestedName: c.meta.name }); c.writable = await h.createWritable(); }
    catch (e) { if (e && e.name === 'AbortError') { btn.disabled = false; return; } c.useMem = true; }
  } else { c.useMem = true; }
  c.t0 = Date.now();
  wsSend(JSON.stringify({ type: 'rready', tid: c.tid }));
  rstat.textContent = c.useMem ? '内存接收中(大文件慎用)...' : '接收中...';
}

async function onRecvChunk(buf) {
  var c = recvCtx; if (!c) return;
  c.received += buf.byteLength;
  var p = c.writeChain.then(function() { if (c.writable) return c.writable.write(buf); c.chunks.push(buf); });
  c.writeChain = p; await p;
  c.written += buf.byteLength;
  recvProg();
  if (c.written - c.lastAck >= ACKEVERY) { c.lastAck = c.written; wsSend(JSON.stringify({ type: 'rack', tid: c.tid, bytes: c.written })); }
}

function recvProg() {
  var c = recvCtx; if (!c) return;
  var pct = c.meta.size ? Math.floor(c.received * 100 / c.meta.size) : 0;
  rbar.style.width = pct + '%';
  var sec = (Date.now() - c.t0) / 1000, sp = sec > 0 ? c.received / sec : 0;
  rstat.textContent = '接收中 ' + pct + '%  ' + fmt(c.received) + ' / ' + fmt(c.meta.size) + '  (' + fmt(sp) + '/s)';
}

async function onRecvEof(m) {
  var c = recvCtx;
  if (!c || m.tid !== c.tid || c.finalizing) return;
  c.finalizing = true;
  await c.writeChain;
  wsSend(JSON.stringify({ type: 'rack', tid: c.tid, bytes: c.written }));
  if (c.writable) {
    await c.writable.close();
  } else {
    var blob = new Blob(c.chunks, { type: c.meta.mime });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a'); a.href = url; a.download = c.meta.name;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(function() { URL.revokeObjectURL(url); }, 10000);
  }
  wsSend(JSON.stringify({ type: 'rdone', tid: c.tid }));
  rbar.style.width = '100%';
  rstat.textContent = '接收完成 ✓  ' + c.meta.name;
  rlist.innerHTML = '';
  recvCtx = null;
}
`;
}