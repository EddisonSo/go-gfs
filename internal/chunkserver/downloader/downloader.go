package downloader

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"os"
	"strconv"
	"github.com/google/uuid"

	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/fanoutcoordinator"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"eddisonso.com/go-gfs/internal/chunkserver/chunkstagingtrackingservice"
	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
	"github.com/golang-jwt/jwt/v5"
)

type FileDownloadService struct {
	ChunkServerConfig csstructs.ChunkServerConfig
	ChunkStagingTrackingService *chunkstagingtrackingservice.ChunkStagingTrackingService
	timeout int
}


func NewFileDownloadService(config csstructs.ChunkServerConfig, timeout int) *FileDownloadService {
	return &FileDownloadService {
		ChunkServerConfig: config,
		ChunkStagingTrackingService: chunkstagingtrackingservice.GetChunkStagingTrackingService(),
		timeout: timeout,
	}
}

func (fds *FileDownloadService) handle(conn net.Conn) {
	defer conn.Close()

	slog.Info("New connection established", "remote_addr", conn.RemoteAddr().String())

	nBytes := make([]byte, 4)
	n, err := conn.Read(nBytes)

	if err != nil {
		slog.Error("Failed to read JWT length", "error", err)
		return
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

	token, err := jwt.ParseWithClaims(jwtTokenString, &csstructs.DownloadRequestClaims{}, secrets.GetSecret)

	if err != nil {
		slog.Error("Failed to parse JWT token", "error", err)
		return
	}

	claims, ok := token.Claims.(*csstructs.DownloadRequestClaims)
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

	stagedchunk := stagedchunk.NewStagedChunk(
		claims.ChunkHandle,
		uuid.New().String(),
		csstructs.Receiving,
		claims.Filesize,
	)
	fds.ChunkStagingTrackingService.AddStagedChunk(stagedchunk)

	ctx := context.TODO()

	coordinator := fanoutcoordinator.NewFanoutCoordinator(conn)
	coordinator.SetStagedChunk(stagedchunk)
	coordinator.AddReplicas(claims.Replicas)
	coordinator.StartFanout(ctx, conn)
}

func (fds *FileDownloadService) ListenAndServe() error {
	if err := os.MkdirAll(fds.ChunkServerConfig.Dir, 0o755); err != nil {
		slog.Error("Failed to create directory", "dir", fds.ChunkServerConfig.Dir, "error", err)
		return err
	}

	ln, err := net.Listen("tcp", fds.ChunkServerConfig.Hostname + ":" + strconv.Itoa(fds.ChunkServerConfig.Port))
	if err != nil {
		slog.Error("Failed to start listener", "address", fds.ChunkServerConfig.Hostname + ":" + strconv.Itoa(fds.ChunkServerConfig.Port), "error", err)
		return err
	}

	slog.Info("FileDownloadService is listening", "address", fds.ChunkServerConfig.Hostname + ":" + strconv.Itoa(fds.ChunkServerConfig.Port))
	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("Failed to accept connection", "error", err)
			continue
		}
		go fds.handle(conn)
	}
}
