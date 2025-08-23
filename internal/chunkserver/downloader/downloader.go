package downloader

import (
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"

	"eddisonso.com/go-gfs/internal/chunkserver/csconfig"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"github.com/golang-jwt/jwt/v5"
)

type FileDownloadService struct {
	ChunkServerConfig csconfig.ChunkServerConfig
	timeout int
}

type ReplicaIdentifier struct {
	ID   string `json:"id"`
	Hostname string `json:"hostname"`
}

type DownloadRequestClaims struct {
    ChunkHandle string `json:"chunk_handle"`
    Operation string `json:"operation"`
    Filesize int64 `json:"file_size"`
    Replicas []ReplicaIdentifier `json:"replicas"`
    Primary ReplicaIdentifier `json:"primary"`
    jwt.RegisteredClaims
}

func NewFileDownloadService(config csconfig.ChunkServerConfig, timeout int) *FileDownloadService {
	return &FileDownloadService {
		ChunkServerConfig: config,
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
		slog.Error("Failed to read JWT length", "bytes_read", n)
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
	if claims.Operation != "download" {
		slog.Error("Invalid operation", "operation", claims.Operation)
		return
	}

	if claims.Filesize <= 0 || claims.Filesize > 2<<26 { //Max 64 MB
		slog.Error("Invalid file size", "file_size", claims.Filesize)
	}

	buf := make([]byte, 1024*1024) // 1 MB buffer
	file, err := os.Open(fds.ChunkServerConfig.Dir + "/" + claims.ChunkHandle)
    
	if err != nil {
		slog.Error("Failed to open file", "file", claims.ChunkHandle, "error", err)
		return
	}
	defer file.Close()

	totalBytes := int64(0)
	for {
		n, err := conn.Read(buf)
		totalBytes += int64(n)
		if n > 0 {
			if _, werr := file.Write(buf[:n]); werr != nil {
				panic(werr)
			}
		}
		if err != nil {           // io.EOF means clean close
			if err == io.EOF { break }
			panic(err)
		}
	}
	if totalBytes != claims.Filesize {
		slog.Warn("File size mismatch", "expected", claims.Filesize, "actual", totalBytes)
		return
	}
	file.Sync()
}

func (fds *FileDownloadService) ListenAndServe() error {
	if err := os.MkdirAll(fds.ChunkServerConfig.Dir, 0o755); err != nil {
		panic(err)
	}

	ln, err := net.Listen("tcp", fds.ChunkServerConfig.Hostname + ":" + strconv.Itoa(fds.ChunkServerConfig.Port))
	if err != nil {
		panic(err)
	}

	slog.Info("FileDownloadService is listening", "address", fds.ChunkServerConfig.Hostname + ":" + strconv.Itoa(fds.ChunkServerConfig.Port))
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go fds.handle(conn)
	}
}
