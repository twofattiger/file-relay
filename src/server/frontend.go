package main

import "fmt"

const WS_CHUNK_SIZE = 512 * 1024
const RTC_CHUNK_SIZE = 256 * 1024
const WINDOW_BYTES = 8 * 1024 * 1024
const ACK_EVERY = 4 * 1024 * 1024

var STUN_SERVERS = []string{
	"stun:stun.cloudflare.com:3478",
	"stun:stun.l.google.com:19302",
}

func getStunServersJSON() string {
	return `[{"urls":"stun:stun.cloudflare.com:3478"},{"urls":"stun:stun.l.google.com:19302"}]`
}

const STYLE = `<style>
*{box-sizing:border-box}body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#0f1115;color:#e6e6e6;display:flex;min-height:100vh;align-items:center;justify-content:center;padding:20px}
.card{width:100%;max-width:520px;background:#171a21;border:1px solid #262b36;border-radius:14px;padding:28px}
h1{font-size:18px;margin:0 0 18px;display:flex;align-items:center;gap:8px}input,button{font-size:16px}
input[type=password],input[type=text]{width:100%;padding:11px 12px;background:#0f1115;border:1px solid #2c3340;border-radius:9px;color:#e6e6e6;margin-bottom:12px}
input[type=file]{margin-bottom:14px}
button{cursor:pointer;padding:11px 16px;border:0;border-radius:9px;background:#3b82f6;color:#fff;font-weight:600}
button:disabled{opacity:.4;cursor:not-allowed}button.ghost{background:#262b36}
.row{display:flex;gap:8px}.row input{flex:1;margin-bottom:0}
#status{margin-top:16px;font-size:14px;color:#9aa4b2;min-height:56px;white-space:pre-line;line-height:1.7}
.barwrap{height:8px;background:#0f1115;border-radius:6px;overflow:hidden;margin-top:14px;border:1px solid #2c3340}
#bar{height:100%;width:0;background:linear-gradient(90deg,#3b82f6,#22d3ee);transition:width .15s}
.hint{font-size:12px;color:#6b7280;margin-top:10px}
.info{background:#0f1115;border:1px solid #2c3340;border-radius:9px;padding:12px;margin-bottom:14px;font-size:14px;display:none}
.opt{display:flex;align-items:center;gap:8px;margin-bottom:14px;font-size:13px;color:#9aa4b2;cursor:pointer;user-select:none}
.opt input{width:16px;height:16px;margin:0;accent-color:#3b82f6;cursor:pointer;flex:none}
.opt input:disabled{cursor:not-allowed;opacity:.5}
.badge{display:none;font-size:12px;font-weight:600;padding:3px 10px;border-radius:999px;background:#262b36;color:#9aa4b2;border:1px solid #2c3340}
.badge.p2p{background:#064e3b;color:#34d399;border-color:#065f46}
.badge.relay{background:#1e293b;color:#60a5fa;border-color:#1e40af}
@media (max-width:480px){.card{padding:20px}body{padding:10px}}
</style>`

const LOGIN_HTML = `<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'>
<title>中继站 · 登录</title>` + STYLE + `
<div class=card><h1>🔐 输入系统密码</h1>
<input id=pw type=password placeholder='密码' autofocus>
<button id=go>进入</button><div id=status></div></div>
<script>
var pw=document.getElementById('pw'),go=document.getElementById('go'),st=document.getElementById('status');
function login(){go.disabled=true;st.textContent='验证中...';
fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pw.value})})
.then(function(r){return r.json();}).then(function(d){if(d.ok){location.href='/';}else{st.textContent=d.error||'失败';go.disabled=false;}})
.catch(function(){st.textContent='网络错误';go.disabled=false;});}
go.onclick=login;pw.addEventListener('keydown',function(e){if(e.key==='Enter')login();});
</script>`

func getSenderHTML() string {
	return `<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'>
<title>中继站 · 发送</title>` + STYLE + `
<div class=card><h1>📤 实时发送文件 <span id=badge class=badge></span></h1>
<input id=file type=file>
<label class=opt><input id=p2p type=checkbox checked> ⚡ 优先 P2P 直连（尝试点对点直连，否则通过服务端中继）</label>
<button id=gen disabled>生成传输链接</button>
<div id=linkbox style='display:none;margin-top:14px'>
<div class=row><input id=link type=text readonly><button id=copy class=ghost>复制</button></div>
<div class=hint>把链接发给对方，对方打开并选择保存位置后开始实时传输（双方需保持页面打开）</div></div>
<div class=barwrap><div id=bar></div></div>
<div id=status></div></div>
<script>` + getSenderJS() + `</script>`
}

func getReceiverHTML() string {
	return `<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'>
<title>中继站 · 接收</title>` + STYLE + `
<div class=card><h1>📥 接收文件 <span id=badge class=badge></span></h1>
<div id=info class=info></div>
<label class=opt><input id=p2p type=checkbox checked> ⚡ 优先 P2P 直连（尝试点对点直连，否则通过服务端中继）</label>
<button id=save disabled>选择保存位置并接收</button>
<div class=barwrap><div id=bar></div></div>
<div id=status>连接中...</div></div>
<script>` + getReceiverJS() + `</script>`
}

const FMT_JS = `function fmt(b){if(b<1024)return b+' B';var u=['KB','MB','GB','TB'],i=-1;do{b=b/1024;i++;}while(b>=1024&&i<u.length-1);return b.toFixed(1)+' '+u[i];}
function fmtTime(s){s=Math.round(s);if(s<60)return s+'s';var m=Math.floor(s/60),r=s%60;if(m<60)return m+'m'+(r?r+'s':'');var h=Math.floor(m/60);m=m%60;return h+'h'+(m?m+'m':'');}`

func getSenderJS() string {
	return FMT_JS + fmt.Sprintf(`
var WS_CHUNK = %d, RTC_CHUNK = %d, WINDOW = %d;
var ICE = %s;
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

function decideTransport() {
  if (metaSent || started || decided) return;
  decided = true;
  clearTimeout(prefsTimer);
  p2pEl.disabled = true;
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
`, WS_CHUNK_SIZE, RTC_CHUNK_SIZE, WINDOW_BYTES, getStunServersJSON())
}

func getReceiverJS() string {
	return FMT_JS + fmt.Sprintf(`
var ACKEVERY = %d;
var ICE = %s;
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
        if (!prefsSent) {
          prefsSent = true;
          p2pEl.disabled = true;
          if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'prefs', p2p: p2pEl.checked }));
        }
      }
      else if (m.type === 'webrtc-offer') {
        if (!p2pEl.checked) return;
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
`, ACK_EVERY, getStunServersJSON())
}
