package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// servePage：需登录的页面统一在未登录时返回登录页（登录成功后 reload 回到当前地址）。
func servePage(w http.ResponseWriter, r *http.Request, authedHTML string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if isAuthed(r) {
		w.Write([]byte(authedHTML))
	} else {
		w.Write([]byte(LOGIN_HTML))
	}
}

// "/"：登录后展示功能选择卡片（分享发送 / 设备互传）
func handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	servePage(w, r, CHOICE_HTML)
}

// "/send"：方式1 分享发送
func handleSend(w http.ResponseWriter, r *http.Request) {
	servePage(w, r, getSenderHTML())
}

// "/room"：方式2 创建房间
func handleRoom(w http.ResponseWriter, r *http.Request) {
	servePage(w, r, ROOM_CREATE_HTML)
}

// "/m/{密码}"：方式2 房间互传。
// 始终返回页面（临时口令在 #hash 里、服务端看不到，鉴权由前端+WS 完成）；
// 把"本次请求是否已登录"注入页面：无 #口令且未登录时，前端走原登录逻辑。
func handleRoomPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(getRoomHTML(isAuthed(r))))
}

// "/r/{id}"：方式1 接收（匿名）
func handleReceiver(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(getReceiverHTML()))
}

type LoginRequest struct {
	Password string `json:"password"`
}

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if password == "" {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "未配置 PASSWORD 环境变量"}, http.StatusInternalServerError)
		return
	}
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "无效的请求格式"}, http.StatusBadRequest)
		return
	}
	if req.Password != password {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "密码错误"}, http.StatusUnauthorized)
		return
	}

	token := makeToken(password)
	secure := ""
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		secure = "; Secure"
	}
	cookieStr := "sess=" + token + "; HttpOnly" + secure + "; SameSite=Lax; Path=/; Max-Age=" + strconv.Itoa(SESSION_TTL)
	w.Header().Set("Set-Cookie", cookieStr)
	writeJSON(w, map[string]interface{}{"ok": true}, http.StatusOK)
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Set-Cookie", "sess=; Path=/; Max-Age=0")
	http.Redirect(w, r, "/", http.StatusFound)
}

func writeJSON(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
