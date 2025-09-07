package downloader

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"os"

	"github.com/google/uuid"

	"eddisonso.com/go-gfs/internal/chunkserver/allocator"
	"eddisonso.com/go-gfs/internal/chunkserver/allocatortrackingservice"
	"eddisonso.com/go-gfs/internal/chunkserver/chunkstagingtrackingservice"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/fanoutcoordinator"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
	"github.com/golang-jwt/jwt/v5"
)

type FileDownloadService struct {
	ChunkServerConfig csstructs.ChunkServerConfig
	ChunkStagingTrackingService *chunkstagingtrackingservice.ChunkStagingTrackingService
	timeout int
}


func NewFileDownloadService(config csstructs.ChunkServerConfig, timeout int) (*FileDownloadService, error) {
	fds := &FileDownloadService {
		ChunkServerConfig: config,
		ChunkStagingTrackingService: chunkstagingtrackingservice.GetChunkStagingTrackingService(),
		timeout: timeout,
	}
	if err := os.MkdirAll(fds.ChunkServerConfig.Dir, 0o755); err != nil {
		slog.Error("Failed to create directory", "dir", fds.ChunkServerConfig.Dir, "error", err)
		return nil, err
	}
	
	return fds, nil
}

func (fds *FileDownloadService) HandleDownload(conn net.Conn) {
	defer conn.Close()

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

	slog.Info("Download request", "chunk_handle", claims.ChunkHandle, "operation", claims.Operation, "filesize", claims.Filesize)
	if claims.Operation != "download" {
		slog.Error("Invalid operation", "operation", claims.Operation)
		return
	}

	if claims.Filesize <= 0 || claims.Filesize > 2<<26 { //Max 64 MB
		slog.Error("Invalid file size", "file_size", claims.Filesize)
	}
	
	ats := allocatortrackingservice.GetAllocatorTrackingService()
	currAllocator, err := ats.GetAllocator(claims.ChunkHandle)
	if err != nil {
		currAllocator = allocator.NewAllocator(64 << 20)
		ats.AddAllocator(claims.ChunkHandle, currAllocator)
	}

	ctxAllocator := context.TODO()
	opId := uuid.New().String()
	currAllocator.Reserve(ctxAllocator, opId, claims.Filesize, 2)
	ats.AddAllocator(claims.ChunkHandle, currAllocator)

	stagedchunk := stagedchunk.NewStagedChunk(
		claims.ChunkHandle,
		opId,
		claims.Filesize,
	)

	fds.ChunkStagingTrackingService.AddStagedChunk(stagedchunk)

	coordinator := fanoutcoordinator.NewFanoutCoordinator(claims.Replicas, stagedchunk)
	coordinator.SetStagedChunk(stagedchunk)
	coordinator.AddReplicas(claims.Replicas)
	coordinator.StartFanout(conn, jwtTokenString)
}
