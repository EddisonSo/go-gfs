package downloader

import (
	"log/slog"
	"net"
	"os"
	"encoding/binary"
	"github.com/golang-jwt/jwt/v5"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
)

type FileDownloadService struct {
	addr    string
	rootDir string
	timeout int
}

type DownloadRequestClaims struct {
    ChunkHandle string `json:"chunk_handle"`
    Operation string `json:"operation"`
    jwt.RegisteredClaims
}

func NewFileDownloadService(address, rootDir string, timeout int) *FileDownloadService {
	return &FileDownloadService{
		addr:    address,
		rootDir: rootDir,
		timeout: timeout,
	}
}

func (fds *FileDownloadService) handle(conn net.Conn) {
	defer conn.Close()

	slog.Info("New connection established", "remote_addr", conn.RemoteAddr().String())

	nBytes := make([]byte, 4)
	n, err := conn.Read(nBytes)

	if err != nil {
		panic(err)
	}

	if n != 4 {
		slog.Error("Failed to read file size", "bytes_read", n)
		return
	}

	tokenSize := binary.BigEndian.Uint32(nBytes)
	
	jwtToken := make([]byte, tokenSize)
	n, err = conn.Read(jwtToken)
	if err != nil {
		slog.Error("Failed to read JWT token", "error", err)
		return
	}

	if n != int(tokenSize) {
		slog.Error("JWT token size mismatch", "expected", tokenSize, "actual", n)
		return
	}

	jwtTokenString := string(jwtToken)

	token, err := jwt.ParseWithClaims(jwtTokenString, &DownloadRequestClaims{}, secrets.GetSecret)

	if err != nil {
		slog.Error("Failed to parse JWT token", "error", err)
		return
	}

	claims, ok := token.Claims.(*DownloadRequestClaims)
	if !ok || !token.Valid {
		slog.Error("Invalid JWT token")
		return
	}

	slog.Info("Download request", "chunk_handle", claims.ChunkHandle, "operation", claims.Operation)
}

func (fds *FileDownloadService) ListenAndServe() error {
	if err := os.MkdirAll(fds.rootDir, 0o755); err != nil {
		panic(err)
	}

	ln, err := net.Listen("tcp", fds.addr)
	if err != nil {
		panic(err)
	}

	slog.Info("FileDownloadService is listening", "address", fds.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go fds.handle(conn)
	}
}
