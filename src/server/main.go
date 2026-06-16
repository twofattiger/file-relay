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
	tlsPort  string
	certFile string
	keyFile  string
	password string
)

func init() {
	flag.StringVar(&port, "port", "8080", "HTTP Server port")
	flag.StringVar(&tlsPort, "tls-port", "", "HTTPS Server port (leave empty to disable)")
	flag.StringVar(&certFile, "cert", "", "Path to SSL certificate file")
	flag.StringVar(&keyFile, "key", "", "Path to SSL key file")
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

	errChan := make(chan error, 2)

	// 启动 HTTP 服务
	if port != "" {
		go func() {
			addr := fmt.Sprintf(":%s", port)
			log.Printf("Starting HTTP server on %s\n", addr)
			errChan <- http.ListenAndServe(addr, nil)
		}()
	}

	// 启动 HTTPS 服务
	if tlsPort != "" && certFile != "" && keyFile != "" {
		go func() {
			addrTLS := fmt.Sprintf(":%s", tlsPort)
			log.Printf("Starting HTTPS server on %s\n", addrTLS)
			errChan <- http.ListenAndServeTLS(addrTLS, certFile, keyFile, nil)
		}()
	} else if tlsPort != "" {
		log.Println("WARNING: tls-port is set but cert or key is missing. HTTPS server will not start.")
	}

	// 阻塞等待任意一个服务退出
	log.Fatalf("Server stopped: %v", <-errChan)
}

// TODO: Authentication
// TODO: WebSocket Handling
// TODO: HTML Serving
