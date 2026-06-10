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

	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/r/", handleReceiver)
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
