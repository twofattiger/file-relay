// ============================================================
// Cloudflare Worker 文件实时中继站（Durable Object · 不落盘）
//   Worker            : 鉴权 / 发页面 / 把 WS 升级请求路由到 DO
//   TransferSession DO: 按 transfer id 配对 sender/receiver 两条 WebSocket 并实时转发
//   ENV PASSWORD      : 登录密码（环境变量/Secret）
// 无需 KV/R2/D1。两端需同时在线；文件字节边传边走，服务端不存储。
// 仅需一次 `npx wrangler deploy`（建立 DO 迁移），之后改代码/环境变量都可在 dashboard 完成。
//
// 协议（peer<->peer，DO 原样转发）:
//   sender->receiver  {type:"meta", name, size, mime, chunkSize}
//   sender->receiver  <binary chunk>          顺序、每块<=chunkSize
//   sender->receiver  {type:"eof"}
//   receiver->sender  {type:"ready"}          已选好保存位置，开始推流
//   receiver->sender  {type:"ack", bytes}     已落盘字节（滑动窗口流控）
//   receiver->sender  {type:"complete"}
// DO 生命周期消息（DO->client）:
//   {type:"role",role} {type:"peer-joined"} {type:"peer-closed"} {type:"error",message}
// ============================================================

import { DurableObject } from "cloudflare:workers";
const CHUNK_SIZE = 1024 * 1024;       // 1 MiB（WS 单条上限已是 32 MiB）
const WINDOW_BYTES = 8 * CHUNK_SIZE;  // 发送方滑动窗口
const ACK_EVERY = 4 * CHUNK_SIZE;     // 接收方每落盘这么多字节回一次 ACK
const SESSION_TTL = 86400;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path.startsWith("/ws/")) {
      const id = path.split("/")[2];
      if (!id) return new Response("bad id", { status: 400 });
      const role = url.searchParams.get("role");
      if (role === "sender" && !(await isAuthed(request, env))) return new Response("unauthorized", { status: 401 });
      const stub = env.TRANSFER.get(env.TRANSFER.idFromName(id));
      return stub.fetch(request);
    }
    if (path === "/api/login" && request.method === "POST") return handleLogin(request, env);
    if (path === "/api/logout") return new Response(null, { status: 302, headers: { Location: "/", "Set-Cookie": "sess=; Path=/; Max-Age=0" } });
    if (path.startsWith("/r/")) return html(RECEIVER_HTML);
    if (path === "/") return html((await isAuthed(request, env)) ? SENDER_HTML : LOGIN_HTML);
    return new Response("Not found", { status: 404 });
  },
};

// ---------------- Durable Object ----------------
export class TransferSession extends DurableObject{
  constructor(state, env) { super(state, env); this.state = state; this.env = env;}

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") return new Response("expected websocket", { status: 426 });
    const url = new URL(request.url);
    const role = url.searchParams.get("role") === "sender" ? "sender" : "receiver";
    const tag = role === "sender" ? "s" : "r";

    if (this.state.getWebSockets(tag).length >= 1) {
      const dup = new WebSocketPair();
      dup[1].accept();
      dup[1].send(JSON.stringify({ type: "error", message: "该角色已有连接" }));
      dup[1].close(1008, "dup");
      return new Response(null, { status: 101, webSocket: dup[0] });
    }

    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    this.state.acceptWebSocket(server, [tag]);
    server.send(JSON.stringify({ type: "role", role }));

    const opp = role === "sender" ? "r" : "s";
    const others = this.state.getWebSockets(opp);
    if (others.length) {
      try { server.send(JSON.stringify({ type: "peer-joined" })); } catch (e) {}
      for (const o of others) { try { o.send(JSON.stringify({ type: "peer-joined" })); } catch (e) {} }
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const isSender = this.state.getTags(ws).includes("s");
    const targets = this.state.getWebSockets(isSender ? "r" : "s");
    if (!targets.length) { try { ws.send(JSON.stringify({ type: "peer-closed" })); } catch (e) {} return; }
    try { targets[0].send(message); } catch (e) { try { ws.send(JSON.stringify({ type: "error", message: "relay failed" })); } catch (_) {} }
  }

  async webSocketClose(ws) {
    const isSender = this.state.getTags(ws).includes("s");
    for (const o of this.state.getWebSockets(isSender ? "r" : "s")) { try { o.send(JSON.stringify({ type: "peer-closed" })); } catch (e) {} }
    try { ws.close(1000); } catch (e) {}
  }

  async webSocketError() {}
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
  "h1{font-size:18px;margin:0 0 18px}input,button{font-size:15px}" +
  "input[type=password],input[type=text]{width:100%;padding:11px 12px;background:#0f1115;border:1px solid #2c3340;border-radius:9px;color:#e6e6e6;margin-bottom:12px}" +
  "input[type=file]{margin-bottom:14px}" +
  "button{cursor:pointer;padding:11px 16px;border:0;border-radius:9px;background:#3b82f6;color:#fff;font-weight:600}" +
  "button:disabled{opacity:.4;cursor:not-allowed}button.ghost{background:#262b36}" +
  ".row{display:flex;gap:8px}.row input{flex:1;margin-bottom:0}" +
  "#status{margin-top:16px;font-size:14px;color:#9aa4b2;min-height:20px}" +
  ".barwrap{height:8px;background:#0f1115;border-radius:6px;overflow:hidden;margin-top:14px;border:1px solid #2c3340}" +
  "#bar{height:100%;width:0;background:linear-gradient(90deg,#3b82f6,#22d3ee);transition:width .15s}" +
  ".hint{font-size:12px;color:#6b7280;margin-top:10px}" +
  ".info{background:#0f1115;border:1px solid #2c3340;border-radius:9px;padding:12px;margin-bottom:14px;font-size:14px;display:none}" +
  "</style>";

const LOGIN_HTML =
  "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>" +
  "<title>中继站 · 登录</title>" + STYLE +
  "<div class=card><h1>🔐 输入系统密码</h1>" +
  "<input id=pw type=password placeholder='密码' autofocus>" +
  "<button id=go>进入</button><div id=status></div></div>" +
  "<script>" +
  "var pw=document.getElementById('pw'),go=document.getElementById('go'),st=document.getElementById('status');" +
  "function login(){go.disabled=true;st.textContent='验证中...';" +
  "fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pw.value})})" +
  ".then(function(r){return r.json();}).then(function(d){if(d.ok){location.href='/';}else{st.textContent=d.error||'失败';go.disabled=false;}})" +
  ".catch(function(){st.textContent='网络错误';go.disabled=false;});}" +
  "go.onclick=login;pw.addEventListener('keydown',function(e){if(e.key==='Enter')login();});" +
  "</script>";

const SENDER_HTML =
  "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>" +
  "<title>中继站 · 发送</title>" + STYLE +
  "<div class=card><h1>📤 实时发送文件</h1>" +
  "<input id=file type=file>" +
  "<button id=gen disabled>生成传输链接</button>" +
  "<div id=linkbox style='display:none;margin-top:14px'>" +
  "<div class=row><input id=link type=text readonly><button id=copy class=ghost>复制</button></div>" +
  "<div class=hint>把链接发给对方，对方打开并选择保存位置后开始实时传输（双方需保持页面打开）</div></div>" +
  "<div class=barwrap><div id=bar></div></div>" +
  "<div id=status></div></div>" +
  "<script>" + SENDER_JS() + "</script>";

const RECEIVER_HTML =
  "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>" +
  "<title>中继站 · 接收</title>" + STYLE +
  "<div class=card><h1>📥 接收文件</h1>" +
  "<div id=info class=info></div>" +
  "<button id=save disabled>选择保存位置并接收</button>" +
  "<div class=barwrap><div id=bar></div></div>" +
  "<div id=status>连接中...</div></div>" +
  "<script>" + RECEIVER_JS() + "</script>";

function FMT_JS() {
  return "function fmt(b){if(b<1024)return b+' B';var u=['KB','MB','GB','TB'],i=-1;do{b=b/1024;i++;}while(b>=1024&&i<u.length-1);return b.toFixed(1)+' '+u[i];}";
}

function SENDER_JS() {
  return (
    FMT_JS() +
    "var CHUNK=" + CHUNK_SIZE + ",WINDOW=" + WINDOW_BYTES + ";" +
    "var ws=null,file=null,id=null,t0=0,done=false;" +
    "var sent=0,acked=0,offset=0,eofSent=false,metaSent=false,started=false,pumping=false,rerun=false;" +
    "var fileEl=document.getElementById('file'),genEl=document.getElementById('gen');" +
    "var st=document.getElementById('status'),bar=document.getElementById('bar');" +
    "var linkbox=document.getElementById('linkbox'),linkEl=document.getElementById('link');" +
    "function S(s){st.textContent=s;}" +
    "fileEl.onchange=function(){file=fileEl.files[0]||null;genEl.disabled=!file;};" +
    "document.getElementById('copy').onclick=function(){linkEl.select();if(navigator.clipboard)navigator.clipboard.writeText(linkEl.value);};" +
    "genEl.onclick=function(){if(!file)return;id=crypto.randomUUID();linkEl.value=location.origin+'/r/'+id;" +
    "linkbox.style.display='block';genEl.disabled=true;fileEl.disabled=true;connect();};" +
    "function reset(){if(done)return;offset=0;sent=0;acked=0;eofSent=false;metaSent=false;started=false;bar.style.width='0%';}" +
    "function connect(){var p=location.protocol==='https:'?'wss':'ws';" +
    "ws=new WebSocket(p+'://'+location.host+'/ws/'+id+'?role=sender');ws.binaryType='arraybuffer';" +
    "ws.onopen=function(){S('已连接中继，等待对方打开链接...');};" +
    "ws.onerror=function(){S('连接错误');};" +
    "ws.onmessage=function(ev){if(typeof ev.data!=='string')return;var m=JSON.parse(ev.data);" +
    "if(m.type==='peer-joined'){if(!metaSent&&!started)sendMeta();}" +
    "else if(m.type==='ready'){started=true;t0=Date.now();S('正在传输...');pump();}" +
    "else if(m.type==='ack'){acked=m.bytes;pump();}" +
    "else if(m.type==='complete'){done=true;bar.style.width='100%';S('传输完成 ✓');try{ws.close();}catch(e){}}" +
    "else if(m.type==='peer-closed'){if(!done){S('对方已断开，等待重新连接...');reset();}}" +
    "else if(m.type==='error'){S('错误: '+(m.message||''));}};}" +
    "function sendMeta(){metaSent=true;S('对方已连接，等待对方确认接收...');" +
    "ws.send(JSON.stringify({type:'meta',name:file.name,size:file.size,mime:file.type||'application/octet-stream',chunkSize:CHUNK}));}" +
    "async function pump(){if(pumping){rerun=true;return;}pumping=true;" +
    "do{rerun=false;while(offset<file.size&&(sent-acked)<WINDOW){" +
    "var end=Math.min(offset+CHUNK,file.size);var buf=await file.slice(offset,end).arrayBuffer();" +
    "ws.send(buf);sent+=buf.byteLength;offset=end;prog();}}while(rerun);pumping=false;" +
    "if(offset>=file.size&&!eofSent){ws.send(JSON.stringify({type:'eof'}));eofSent=true;}}" +
    "function prog(){var pct=file.size?Math.floor(sent*100/file.size):0;bar.style.width=pct+'%';" +
    "var sec=(Date.now()-t0)/1000,sp=sec>0?sent/sec:0;S('传输中 '+pct+'%  '+fmt(sent)+' / '+fmt(file.size)+'  ('+fmt(sp)+'/s)');}"
  );
}

function RECEIVER_JS() {
  return (
    FMT_JS() +
    "var ACKEVERY=" + ACK_EVERY + ";" +
    "var id=location.pathname.split('/')[2];" +
    "var ws=null,meta=null,writable=null,useMem=false,chunks=[],writeChain=Promise.resolve();" +
    "var received=0,written=0,lastAck=0,t0=0,finalizing=false;" +
    "var info=document.getElementById('info'),save=document.getElementById('save');" +
    "var st=document.getElementById('status'),bar=document.getElementById('bar');" +
    "function S(s){st.textContent=s;}" +
    "function connect(){var p=location.protocol==='https:'?'wss':'ws';" +
    "ws=new WebSocket(p+'://'+location.host+'/ws/'+id+'?role=receiver');ws.binaryType='arraybuffer';" +
    "ws.onopen=function(){S('已连接，等待发送方...');};ws.onerror=function(){S('连接错误');};" +
    "ws.onmessage=function(ev){if(typeof ev.data==='string'){var m=JSON.parse(ev.data);" +
    "if(m.type==='peer-joined'){S('发送方在线，等待文件信息...');}" +
    "else if(m.type==='meta'){meta=m;info.style.display='block';info.textContent='文件: '+m.name+'  ('+fmt(m.size)+')';" +
    "save.disabled=false;S('点击下方按钮选择保存位置');}" +
    "else if(m.type==='eof'){finalize();}" +
    "else if(m.type==='peer-closed'){S('发送方已断开');}" +
    "else if(m.type==='error'){S('错误: '+(m.message||''));}" +
    "}else{onChunk(ev.data);}};}" +
    "save.onclick=async function(){if(!meta)return;" +
    "if(window.showSaveFilePicker){try{var h=await showSaveFilePicker({suggestedName:meta.name});writable=await h.createWritable();}" +
    "catch(e){if(e&&e.name==='AbortError')return;useMem=true;}}else{useMem=true;}" +
    "save.disabled=true;t0=Date.now();ws.send(JSON.stringify({type:'ready'}));" +
    "S(useMem?'内存接收中(大文件慎用)...':'开始接收...');};" +
    "async function onChunk(buf){received+=buf.byteLength;" +
    "var p=writeChain.then(function(){if(writable)return writable.write(buf);chunks.push(buf);});writeChain=p;" +
    "await p;written+=buf.byteLength;prog();" +
    "if(written-lastAck>=ACKEVERY){lastAck=written;ws.send(JSON.stringify({type:'ack',bytes:written}));}}" +
    "function prog(){var pct=meta&&meta.size?Math.floor(received*100/meta.size):0;bar.style.width=pct+'%';" +
    "var sec=(Date.now()-t0)/1000,sp=sec>0?received/sec:0;S('接收中 '+pct+'%  '+fmt(received)+' / '+fmt(meta?meta.size:0)+'  ('+fmt(sp)+'/s)');}" +
    "async function finalize(){if(finalizing)return;finalizing=true;await writeChain;" +
    "ws.send(JSON.stringify({type:'ack',bytes:written}));" +
    "if(writable){await writable.close();}else{var blob=new Blob(chunks,{type:meta.mime});" +
    "var url=URL.createObjectURL(blob);var a=document.createElement('a');a.href=url;a.download=meta.name;" +
    "document.body.appendChild(a);a.click();a.remove();setTimeout(function(){URL.revokeObjectURL(url);},10000);}" +
    "ws.send(JSON.stringify({type:'complete'}));bar.style.width='100%';S('接收完成 ✓  '+meta.name);try{ws.close();}catch(e){}}" +
    "connect();"
  );
}