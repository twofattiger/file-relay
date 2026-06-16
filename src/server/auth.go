package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func makeToken(secret string) string {
	exp := time.Now().UnixMilli() + SESSION_TTL*1000
	expStr := strconv.FormatInt(exp, 10)
	sig := hmacSHA256(secret, expStr)
	return fmt.Sprintf("%s.%s", expStr, sig)
}

func verifyToken(secret, token string) bool {
	if token == "" {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return false
	}
	expStr, sig := parts[0], parts[1]
	exp, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil {
		return false
	}
	if exp < time.Now().UnixMilli() {
		return false
	}
	expectedSig := hmacSHA256(secret, expStr)
	return timingEq(sig, expectedSig)
}

func hmacSHA256(secret, msg string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(msg))
	return b64url(h.Sum(nil))
}

func b64url(bytes []byte) string {
	encoded := base64.StdEncoding.EncodeToString(bytes)
	encoded = strings.ReplaceAll(encoded, "+", "-")
	encoded = strings.ReplaceAll(encoded, "/", "_")
	encoded = strings.TrimRight(encoded, "=")
	return encoded
}

func timingEq(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var r byte
	for i := 0; i < len(a); i++ {
		r |= a[i] ^ b[i]
	}
	return r == 0
}

func isAuthed(r *http.Request) bool {
	if password == "" {
		return true // If no password configured, always auth
	}
	cookie, err := r.Cookie("sess")
	if err != nil {
		return false
	}
	return verifyToken(password, cookie.Value)
}
