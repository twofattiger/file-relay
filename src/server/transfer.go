package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Client 包装一条 WebSocket 连接（写入加锁，保证并发安全），对应 worker 中一条已 accept 的 ws。
type Client struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
	sess    *Session
	role    string // "s"（sender/relay 的 s 槽）| "r"（receiver/relay 的 r 槽）| "peer"
	pid     string // 方式2 mesh：稳定设备 id
	cid     string // 客户端 id（刷新/断线重连复用 pid/槽位）
	name    string // 方式2 mesh：设备名（受 sess.mu 保护）
	// replaced：被同一 cid 的新连接顶替（刷新/重连）。被顶替的旧连接关闭时不通知对端，
	// 也不再占用名册/槽位（避免对端误判“离开”）。
	replaced bool
}

func (c *Client) WriteMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteMessage(messageType, data)
}

func (c *Client) WriteJSON(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.WriteMessage(websocket.TextMessage, b)
}

// Session 对应一个 transfer id（worker 中由 idFromName(id) 得到的一个 Durable Object 实例）。
// 既承载方式1/relay 的 s/r 两端转发，也承载方式2 mesh 的多个 peer 连接（名册 + 信令）。
type Session struct {
	id    string
	mu    sync.Mutex
	s     *Client   // 方式1/relay 的 sender 槽
	r     *Client   // 方式1/relay 的 receiver 槽
	peers []*Client // 方式2 mesh 的 peer 连接
}

var (
	sessions = make(map[string]*Session)
	sessMu   sync.Mutex
)

func getSession(id string) *Session {
	sessMu.Lock()
	defer sessMu.Unlock()
	if s, ok := sessions[id]; ok {
		return s
	}
	s := &Session{id: id}
	sessions[id] = s
	return s
}

func removeSession(id string) {
	sessMu.Lock()
	defer sessMu.Unlock()
	delete(sessions, id)
}

// removeSessionIfEmpty：会话中已无任何连接时从全局表移除，避免空会话泄漏。
func (s *Session) removeSessionIfEmpty() {
	s.mu.Lock()
	empty := s.s == nil && s.r == nil && len(s.peers) == 0
	s.mu.Unlock()
	if empty {
		removeSession(s.id)
	}
}

func genUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		r = r[:n]
	}
	return string(r)
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 路径形如 /ws/{id}?role={sender|receiver|peer|relay}
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 3 || pathParts[2] == "" {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	id := pathParts[2]
	role := r.URL.Query().Get("role")

	// 方式1发送方(sender)、方式2房间端(peer)、方式2配对中继(relay) 均需登录；方式1接收方(receiver) 匿名
	if (role == "sender" || role == "peer" || role == "relay") && !isAuthed(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	rawConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}

	sess := getSession(id)
	c := &Client{conn: rawConn, sess: sess, cid: r.URL.Query().Get("cid")}

	switch role {
	case "peer": // 方式2：多端房间主会话（mesh 信令 + 名册）
		sess.acceptPeer(c)
	case "relay": // 方式2：某一对设备的中继配对（P2P 失败时兜底）
		sess.acceptRelay(c)
	default: // 方式1：固定 sender(s)/receiver(r) 两端，服务端充当中继/信令管道
		if role == "sender" {
			c.role = "s"
		} else {
			c.role = "r"
		}
		sess.acceptSR(c)
	}
}

// ---------------- 拒绝/告知错误 ----------------
// 发一条 error 后用正常关闭码关闭，避免客户端 onerror 覆盖掉具体错误文案。
func reject(c *Client, message string) {
	_ = c.WriteJSON(map[string]string{"type": "error", "message": message})
	_ = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "rejected"))
	c.conn.Close()
}

// ============================================================
// 方式1 / relay：固定 s/r 两端，二进制与信令一律原样转发给对端
// ============================================================
func (s *Session) acceptSR(c *Client) {
	s.mu.Lock()
	if c.role == "s" {
		if s.s != nil {
			s.mu.Unlock()
			reject(c, "该角色已有连接")
			s.removeSessionIfEmpty()
			return
		}
		s.s = c
	} else {
		// transfer id 由发送方创建并先占 s 槽；接收方连入时若没有发送方在线，说明链接无效或发送方已离开。
		if s.s == nil {
			s.mu.Unlock()
			reject(c, "链接无效，或发送方不在线/已离开")
			s.removeSessionIfEmpty()
			return
		}
		if s.r != nil {
			s.mu.Unlock()
			reject(c, "该角色已有连接")
			s.removeSessionIfEmpty()
			return
		}
		s.r = c
	}
	var other *Client
	if c.role == "s" {
		other = s.r
	} else {
		other = s.s
	}
	s.mu.Unlock()

	// 告知本端角色（worker：仅方式1 sender/receiver 发 role；relay 不发）
	roleName := "receiver"
	if c.role == "s" {
		roleName = "sender"
	}
	_ = c.WriteJSON(map[string]string{"type": "role", "role": roleName})

	if other != nil {
		notifyPeerJoined(c)
		notifyPeerJoined(other)
	}

	go s.readLoopSR(c)
}

// 方式2 中继配对：独立会话(房间_pidA_pidB)，仅一对设备占 s/r 两端，二进制天然路由到对端。
func (s *Session) acceptRelay(c *Client) {
	s.mu.Lock()
	// 同一端刷新/重连（cid 相同）：复用其 s/r 槽位
	reclaimed := ""
	if c.cid != "" {
		if s.s != nil && s.s.cid == c.cid {
			reclaimed = "s"
			s.s.replaced = true
			s.s.conn.Close()
			s.s = nil
		}
		if s.r != nil && s.r.cid == c.cid {
			reclaimed = "r"
			s.r.replaced = true
			s.r.conn.Close()
			s.r = nil
		}
	}
	var tag string
	switch {
	case reclaimed != "":
		tag = reclaimed
	case s.s == nil:
		tag = "s"
	case s.r == nil:
		tag = "r"
	default:
		s.mu.Unlock()
		reject(c, "中继配对已满")
		s.removeSessionIfEmpty()
		return
	}
	c.role = tag
	if tag == "s" {
		s.s = c
	} else {
		s.r = c
	}
	var other *Client
	if tag == "s" {
		other = s.r
	} else {
		other = s.s
	}
	s.mu.Unlock()

	if other != nil { // 两端齐 → 双方就绪
		notifyPeerJoined(c)
		notifyPeerJoined(other)
	}

	go s.readLoopSR(c)
}

func (s *Session) readLoopSR(c *Client) {
	defer s.cleanupSR(c)
	for {
		msgType, msgData, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		s.mu.Lock()
		var target *Client
		if c.role == "s" {
			target = s.r
		} else {
			target = s.s
		}
		s.mu.Unlock()

		if target == nil {
			notifyPeerClosed(c)
			continue
		}
		if err := target.WriteMessage(msgType, msgData); err != nil {
			_ = c.WriteJSON(map[string]string{"type": "error", "message": "relay failed"})
		}
	}
}

func (s *Session) cleanupSR(c *Client) {
	s.mu.Lock()
	replaced := c.replaced
	var other *Client
	if c.role == "s" {
		if s.s == c {
			s.s = nil
		}
		other = s.r
	} else {
		if s.r == c {
			s.r = nil
		}
		other = s.s
	}
	empty := s.s == nil && s.r == nil && len(s.peers) == 0
	s.mu.Unlock()

	c.conn.Close()
	if replaced { // 被新连接顶替：不通知对端
		return
	}
	if other != nil {
		notifyPeerClosed(other)
	}
	if empty {
		removeSession(s.id)
	}
}

// ============================================================
// 方式2 mesh：每个连接分配稳定 pid，维护名册；文件走端到端 WebRTC，不经服务端。
// ============================================================
func (s *Session) acceptPeer(c *Client) {
	s.mu.Lock()
	// 同一客户端(cid)的旧连接（刷新/断线重连）：复用其 pid/name，标记 replaced 后关闭
	pid, name := "", ""
	if c.cid != "" {
		for _, old := range s.peers {
			if old.cid == c.cid {
				if old.pid != "" {
					pid = old.pid
				}
				if old.name != "" {
					name = old.name
				}
				old.replaced = true
				old.conn.Close()
			}
		}
	}
	if pid == "" {
		pid = genUUID()
	}
	c.role = "peer"
	c.pid = pid
	c.name = name
	s.peers = append(s.peers, c)
	s.mu.Unlock()

	_ = c.WriteJSON(map[string]string{"type": "self", "pid": pid})
	s.broadcastRoster(nil)

	go s.readLoopPeer(c)
}

func (s *Session) readLoopPeer(c *Client) {
	defer s.cleanupPeer(c)
	for {
		msgType, msgData, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		if msgType != websocket.TextMessage { // 主会话只走 JSON 信令；文件二进制走 P2P 不到这里
			continue
		}
		s.peerMessage(c, msgData)
	}
}

type peerMsg struct {
	Type string          `json:"type"`
	Name string          `json:"name"`
	To   string          `json:"to"`
	Data json.RawMessage `json:"data"`
}

// 只中继 JSON 信令：rename 改名 / signal 定向 WebRTC 信令
func (s *Session) peerMessage(c *Client, raw []byte) {
	var m peerMsg
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	switch {
	case m.Type == "rename":
		s.mu.Lock()
		c.name = truncateRunes(m.Name, 40)
		s.mu.Unlock()
		s.broadcastRoster(nil)
	case m.Type == "signal" && m.To != "":
		out, _ := json.Marshal(map[string]interface{}{"type": "signal", "from": c.pid, "data": m.Data})
		s.mu.Lock()
		targets := make([]*Client, 0, 1)
		for _, p := range s.peers {
			if p.pid == m.To && !p.replaced {
				targets = append(targets, p)
			}
		}
		s.mu.Unlock()
		for _, t := range targets {
			_ = t.WriteMessage(websocket.TextMessage, out)
		}
	}
}

func (s *Session) cleanupPeer(c *Client) {
	s.mu.Lock()
	replaced := c.replaced
	for i, p := range s.peers {
		if p == c {
			s.peers = append(s.peers[:i], s.peers[i+1:]...)
			break
		}
	}
	empty := s.s == nil && s.r == nil && len(s.peers) == 0
	s.mu.Unlock()

	c.conn.Close()
	if !replaced { // 被顶替的旧连接不广播离线（否则刷新重连会让对端误判“离开”）
		s.broadcastRoster(nil)
	}
	if empty {
		removeSession(s.id)
	}
}

// 当前在线设备名册（排除被替换的旧连接，可额外排除一个正在关闭的连接）并广播给所有 peer。
func (s *Session) broadcastRoster(exclude *Client) {
	s.mu.Lock()
	peersList := make([]map[string]string, 0, len(s.peers))
	targets := make([]*Client, 0, len(s.peers))
	for _, p := range s.peers {
		if p == exclude || p.replaced {
			continue
		}
		if p.pid != "" {
			peersList = append(peersList, map[string]string{"pid": p.pid, "name": p.name})
		}
		targets = append(targets, p)
	}
	s.mu.Unlock()

	msg, _ := json.Marshal(map[string]interface{}{"type": "roster", "peers": peersList})
	for _, p := range targets {
		_ = p.WriteMessage(websocket.TextMessage, msg)
	}
}

func notifyPeerJoined(c *Client) {
	if c == nil {
		return
	}
	_ = c.WriteJSON(map[string]string{"type": "peer-joined"})
}

func notifyPeerClosed(c *Client) {
	if c == nil {
		return
	}
	_ = c.WriteJSON(map[string]string{"type": "peer-closed"})
}
