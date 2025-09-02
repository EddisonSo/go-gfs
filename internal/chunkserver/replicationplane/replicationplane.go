package replicationplane

import (
	"encoding/binary"
	"log/slog"
	"net"
	"strconv"
	"sync"

	"eddisonso.com/go-gfs/internal/chunkserver/chunkstagingtrackingservice"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"google.golang.org/protobuf/proto"
	pb "eddisonso.com/go-gfs/gen/chunkreplication"
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

func (rp *ReplicationPlane) handleReplication(conn net.Conn) {
	defer conn.Close()

	lengthBytes := make([]byte, 4)
	n, err := conn.Read(lengthBytes)
	if err != nil {
		slog.Error("Replication: Failed to read replication msg length", "error", err)
		return
	}

	if n != 4 {
		slog.Error("Replication: Invalid replication msg length", "bytes_read", n)
		return
	}

	msgLength := binary.BigEndian.Uint32(lengthBytes)
	msgBytes := make([]byte, msgLength)

	n, err = conn.Read(msgBytes)
	if err != nil {
		slog.Error("Replication: Failed to read replication msg", "error", err)
		return
	}
	if n != int(msgLength) {
		slog.Error("Replication: Invalid replication msg length", "expected", msgLength, "bytes_read", n)
		return
	}

	var msg pb.ReplicationRequest
	err = proto.Unmarshal(msgBytes, &msg)
	if err != nil {
		slog.Error("Replication: Failed to unmarshal replication msg", "error", err)
		return
	}
	
	slog.Info("Replication: Received replication request", "chunkHandle", msg.GetChunkHandle(), "opId", msg.GetOpId(), "length", msg.GetLength())
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

		go rp.handleReplication(conn)

		slog.Info("Replication: New connection established", "remote_addr", conn.RemoteAddr().String())
	}
}
