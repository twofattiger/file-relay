package main

import (
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

type SafeConn struct {
	*websocket.Conn
	writeMu sync.Mutex
}

func (c *SafeConn) WriteMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.Conn.WriteMessage(messageType, data)
}

func (c *SafeConn) WriteJSON(v interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.Conn.WriteJSON(v)
}

type TransferSession struct {
	id       string
	sender   *SafeConn
	receiver *SafeConn
	mu       sync.Mutex
}

var (
	sessions = make(map[string]*TransferSession)
	sessMu   sync.Mutex
)

func getSession(id string) *TransferSession {
	sessMu.Lock()
	defer sessMu.Unlock()
	if s, ok := sessions[id]; ok {
		return s
	}
	s := &TransferSession{id: id}
	sessions[id] = s
	return s
}

func removeSession(id string) {
	sessMu.Lock()
	defer sessMu.Unlock()
	delete(sessions, id)
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Path format: /ws/{id}?role={sender|receiver}
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 3 {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	id := pathParts[2]
	if id == "" {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}

	role := r.URL.Query().Get("role")
	if role == "sender" && !isAuthed(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if role != "sender" && role != "receiver" {
		http.Error(w, "bad role", http.StatusBadRequest)
		return
	}

	rawConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}
	conn := &SafeConn{Conn: rawConn}

	sess := getSession(id)
	sess.mu.Lock()

	isSender := role == "sender"

	if isSender {
		if sess.sender != nil {
			sess.mu.Unlock()
			sendError(conn, "该角色已有连接", websocket.ClosePolicyViolation)
			return
		}
		sess.sender = conn
	} else {
		if sess.receiver != nil {
			sess.mu.Unlock()
			sendError(conn, "该角色已有连接", websocket.ClosePolicyViolation)
			return
		}
		sess.receiver = conn
	}

	// Tell the new client its role
	_ = conn.WriteJSON(map[string]string{"type": "role", "role": role})

	// Notify both if the other is present
	hasPeer := (isSender && sess.receiver != nil) || (!isSender && sess.sender != nil)
	sess.mu.Unlock()

	if hasPeer {
		notifyPeerJoined(sess.sender)
		notifyPeerJoined(sess.receiver)
	}

	// Pump messages
	go pumpMessages(sess, conn, isSender)
}

func pumpMessages(sess *TransferSession, conn *SafeConn, isSender bool) {
	defer func() {
		sess.mu.Lock()
		if isSender {
			sess.sender = nil
		} else {
			sess.receiver = nil
		}
		
		// Determine peer before unlock
		var peer *SafeConn
		if isSender {
			peer = sess.receiver
		} else {
			peer = sess.sender
		}
		isEmpty := sess.sender == nil && sess.receiver == nil
		sess.mu.Unlock()

		if peer != nil {
			notifyPeerClosed(peer)
		}
		
		conn.Close()

		if isEmpty {
			removeSession(sess.id)
		}
	}()

	for {
		msgType, msgData, err := conn.ReadMessage()
		if err != nil {
			break
		}

		sess.mu.Lock()
		var peer *SafeConn
		if isSender {
			peer = sess.receiver
		} else {
			peer = sess.sender
		}
		sess.mu.Unlock()

		if peer == nil {
			notifyPeerClosed(conn)
			continue
		}

		err = peer.WriteMessage(msgType, msgData)
		if err != nil {
			// If relay fails, notify sender/receiver
			sendError(conn, "relay failed", websocket.CloseNormalClosure)
		}
	}
}

func sendError(conn *SafeConn, msg string, code int) {
	_ = conn.WriteJSON(map[string]string{"type": "error", "message": msg})
	_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(code, "dup"))
	conn.Close()
}

func notifyPeerJoined(conn *SafeConn) {
	if conn == nil {
		return
	}
	_ = conn.WriteJSON(map[string]string{"type": "peer-joined"})
}

func notifyPeerClosed(conn *SafeConn) {
	if conn == nil {
		return
	}
	_ = conn.WriteJSON(map[string]string{"type": "peer-closed"})
}
