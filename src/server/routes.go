package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if isAuthed(r) {
		w.Write([]byte(getSenderHTML()))
	} else {
		w.Write([]byte(LOGIN_HTML))
	}
}

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
