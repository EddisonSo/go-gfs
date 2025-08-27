package replicationplane

import (
	"encoding/binary"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"github.com/golang-jwt/jwt/v5"

	"eddisonso.com/go-gfs/internal/chunkserver/chunkstagingtrackingservice"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
)

type ReplicationPlane struct {
	Config csstructs.ChunkServerConfig
	ChunkStagingTrackingService *chunkstagingtrackingservice.ChunkStagingTrackingService
}

var mux = &sync.Mutex{}
var instance *ReplicationPlane

func NewReplicationPlane(config csstructs.ChunkServerConfig) *ReplicationPlane {
	if instance == nil {
		mux.Lock()
		defer mux.Unlock()
		if instance == nil {
			instance = &ReplicationPlane {
				Config: config,
				ChunkStagingTrackingService: chunkstagingtrackingservice.GetChunkStagingTrackingService(),
			}
			return instance
		} else {
			slog.Warn("Replication: StagingRelicator instance already exists, returning existing instance")
		
		}
	} else {
		slog.Warn("Replication: StagingRelicator instance already exists, returning existing instance")
	}

	return instance
}

func (rp *ReplicationPlane) Start() {
	ln, err := net.Listen("tcp", rp.Config.Hostname + ":" + strconv.Itoa(rp.Config.ReplicationPort))
	if err != nil {
		slog.Error("Replication: Failed to start listener", "address", rp.Config.Hostname + ":" + strconv.Itoa(rp.Config.ReplicationPort), "error", err)
	}

	slog.Info("Replication: Listening for replication requests", "address", rp.Config.Hostname + ":" + strconv.Itoa(rp.Config.ReplicationPort))

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("Replication: Failed to accept connection", "error", err)
			continue
		}
		defer conn.Close()

		slog.Info("Replication: New connection established", "remote_addr", conn.RemoteAddr().String())
		buf := make([]byte, 4)

		n, err := conn.Read(buf)
		if err != nil || n != 4 {
			slog.Error("Replication: Failed to read action from connection", "error", err)
			continue
		}

		tokenLength := binary.BigEndian.Uint32(buf)
		jwtToken := make([]byte, tokenLength)
		n, err = conn.Read(jwtToken)
		if err != nil || n != int(tokenLength) {
			slog.Error("Replication: Failed to read JWT token", "error", err, "expected_length", tokenLength, "actual_length", n)
			continue
		}

		slog.Info("Replication: Received JWT token", "token", jwtToken)
		jwtTokenStr := string(jwtToken)
		
		token, err := jwt.ParseWithClaims(jwtTokenStr, &csstructs.DownloadRequestClaims{}, secrets.GetSecret)

		if err != nil {
			slog.Error("Failed to parse JWT token", "error", err)
			return
		}

		claims, ok := token.Claims.(*csstructs.DownloadRequestClaims)
		if !ok || !token.Valid {
			slog.Error("Invalid JWT token")
			return
		}

		slog.Info("Replication: Replication request", "chunk_handle", claims.ChunkHandle, "operation", claims.Operation)
		if claims.Operation != "download" {
			slog.Error("Invalid operation", "operation", claims.Operation)
			return
		}

		if claims.Filesize <= 0 || claims.Filesize > 2<<26 { //Max 64 MB
			slog.Error("Invalid file size", "file_size", claims.Filesize)
		}
	}
}
