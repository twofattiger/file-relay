package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
)

// Constants
const SESSION_TTL = 86400

var (
	port     string
	password string
)

func init() {
	flag.StringVar(&port, "port", "8080", "Server port")
	flag.StringVar(&password, "pwd", "", "Admin password (overrides PASSWORD env var)")
}

func main() {
	flag.Parse()

	if password == "" {
		password = os.Getenv("PASSWORD")
	}
	if password == "" {
		log.Println("WARNING: PASSWORD is not set. Access to sender role is open.")
	}

	http.HandleFunc("/", handleRoot)       // 功能选择页（登录后）
	http.HandleFunc("/send", handleSend)   // 方式1：分享发送
	http.HandleFunc("/room", handleRoom)   // 方式2：创建房间
	http.HandleFunc("/m/", handleRoomPage) // 方式2：房间互传
	http.HandleFunc("/r/", handleReceiver) // 方式1：接收（匿名）
	http.HandleFunc("/api/login", handleLogin)
	http.HandleFunc("/api/logout", handleLogout)
	http.HandleFunc("/ws/", handleWebSocket)

	addr := fmt.Sprintf(":%s", port)
	log.Printf("Starting File Relay server on %s\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// TODO: Authentication
// TODO: WebSocket Handling
// TODO: HTML Serving
