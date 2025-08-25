package replicationplane

import (
	"encoding/binary"
	"log/slog"
	"net"
	"strconv"
	"sync"

	"eddisonso.com/go-gfs/internal/chunkserver/chunkstagingtrackingservice"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
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

		slog.Info("Replication: Received JWT token", jwtToken)
	}

}
