package uploader

import (
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"

	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"github.com/golang-jwt/jwt/v5"
)

type FileUploadService struct {
	ChunkServerConfig csstructs.ChunkServerConfig
}

func NewFileUploadService(config csstructs.ChunkServerConfig) *FileUploadService {
	return &FileUploadService{
		ChunkServerConfig: config,
	}
}

func (fus *FileUploadService) HandleUpload(conn net.Conn) {
	defer conn.Close()

	// Read JWT length
	nBytes := make([]byte, 4)
	n, err := conn.Read(nBytes)
	if err != nil {
		slog.Error("Failed to read JWT length", "error", err)
		fus.sendError(conn, csstructs.ErrInvalidRequest, "failed to read JWT length")
		return
	}

	if n != 4 {
		slog.Error("Failed to read JWT length", "bytes_read", n)
		fus.sendError(conn, csstructs.ErrInvalidRequest, "failed to read JWT length")
		return
	}

	tokenSize := binary.BigEndian.Uint32(nBytes)

	// Read JWT token
	jwtToken := make([]byte, tokenSize)
	n, err = conn.Read(jwtToken)
	if err != nil {
		slog.Error("Failed to read JWT token", "error", err)
		fus.sendError(conn, csstructs.ErrInvalidRequest, "failed to read JWT token")
		return
	}

	if n != int(tokenSize) {
		slog.Error("JWT token size mismatch", "expected", tokenSize, "actual", n)
		fus.sendError(conn, csstructs.ErrInvalidRequest, "JWT token size mismatch")
		return
	}

	// Parse JWT
	token, err := jwt.ParseWithClaims(string(jwtToken), &csstructs.UploadRequestClaims{}, secrets.GetSecret)
	if err != nil {
		slog.Error("Failed to parse JWT token", "error", err)
		fus.sendError(conn, csstructs.ErrInvalidRequest, "invalid JWT token")
		return
	}

	claims, ok := token.Claims.(*csstructs.UploadRequestClaims)
	if !ok || !token.Valid {
		slog.Error("Invalid JWT claims")
		fus.sendError(conn, csstructs.ErrInvalidRequest, "invalid JWT claims")
		return
	}

	if claims.Operation != "upload" {
		slog.Error("Invalid operation", "operation", claims.Operation)
		fus.sendError(conn, csstructs.ErrInvalidRequest, "invalid operation")
		return
	}

	slog.Info("Read request", "chunk_handle", claims.ChunkHandle)

	// Build file path
	chunkFilePath := filepath.Join(fus.ChunkServerConfig.Dir, claims.ChunkHandle)

	// Check file exists and get size
	fileInfo, err := os.Stat(chunkFilePath)
	if os.IsNotExist(err) {
		slog.Error("Chunk not found", "path", chunkFilePath)
		fus.sendError(conn, csstructs.ErrChunkNotFound, "chunk not found")
		return
	}
	if err != nil {
		slog.Error("Failed to stat chunk file", "error", err)
		fus.sendError(conn, csstructs.ErrReadFailure, "failed to access chunk")
		return
	}

	// Open file for reading
	file, err := os.Open(chunkFilePath)
	if err != nil {
		slog.Error("Failed to open chunk file", "error", err)
		fus.sendError(conn, csstructs.ErrReadFailure, "failed to open chunk")
		return
	}
	defer file.Close()

	// Send success response
	if _, err := conn.Write([]byte{1}); err != nil {
		slog.Error("Failed to send success status", "error", err)
		return
	}

	// Send file size
	sizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBytes, uint64(fileInfo.Size()))
	if _, err := conn.Write(sizeBytes); err != nil {
		slog.Error("Failed to send file size", "error", err)
		return
	}

	// Stream file contents
	bytesSent, err := io.Copy(conn, file)
	if err != nil {
		slog.Error("Failed to stream chunk data", "error", err)
		return
	}

	slog.Info("Read complete", "chunk_handle", claims.ChunkHandle, "bytes_sent", bytesSent)
}

func (fus *FileUploadService) sendError(conn net.Conn, code csstructs.ReadErrorCode, message string) {
	// Status: failure
	conn.Write([]byte{0})

	// Error code
	codeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(codeBytes, uint32(code))
	conn.Write(codeBytes)

	// Message length
	msgBytes := []byte(message)
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(msgBytes)))
	conn.Write(lenBytes)

	// Message
	conn.Write(msgBytes)
}
