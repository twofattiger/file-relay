#!/bin/bash

# Exit on any error
set -e

# Setup directories
BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$(dirname "$BIN_DIR")"
OUT_DIR="$SRC_DIR/out"
SERVER_DIR="$SRC_DIR/server"

# Ensure output directory exists
mkdir -p "$OUT_DIR"

# Ensure we have the go compiler in path
if ! command -v go &> /dev/null; then
    echo "Error: 'go' command not found. Please ensure Go is installed and in your PATH."
    exit 1
fi

echo "Building file_relay server cross-platform..."
echo "Source dir: $SERVER_DIR"
echo "Output dir: $OUT_DIR"

cd "$SERVER_DIR" || { echo "Failed to cd to $SERVER_DIR"; exit 1; }

# Download dependencies if needed
echo "Running go mod tidy..."
GOPROXY=https://goproxy.cn,direct go mod tidy

build() {
    local os=$1
    local arch=$2
    local output_name=$3

    echo "Building for $os/$arch..."
    GOOS=$os GOARCH=$arch go build -o "$OUT_DIR/$output_name" .
    if [ $? -eq 0 ]; then
        echo "  -> Success: $OUT_DIR/$output_name"
    else
        echo "  -> Failed"
        exit 1
    fi
}

# Windows 64-bit
build "windows" "amd64" "file_relay_windows_amd64.exe"

# Linux amd64 (64-bit)
build "linux" "amd64" "file_relay_linux_amd64"

# Linux arm64 (aarch64)
build "linux" "arm64" "file_relay_linux_arm64"

# macOS Intel (amd64)
build "darwin" "amd64" "file_relay_darwin_amd64"

# macOS Apple Silicon (arm64)
build "darwin" "arm64" "file_relay_darwin_arm64"

echo "All builds completed successfully! Artifacts are in $OUT_DIR/"
