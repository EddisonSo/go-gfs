package downloader

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/google/uuid"

	"eddisonso.com/go-gfs/internal/chunkserver/allocator"
	"eddisonso.com/go-gfs/internal/chunkserver/allocatortrackingservice"
	"eddisonso.com/go-gfs/internal/chunkserver/chunkstagingtrackingservice"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/fanoutcoordinator"
	"eddisonso.com/go-gfs/internal/chunkserver/masterclient"
	"eddisonso.com/go-gfs/internal/chunkserver/replicationclient"
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

	slog.Info("Download request", "chunk_handle", claims.ChunkHandle, "operation", claims.Operation, "filesize", claims.Filesize, "offset", claims.Offset)
	if claims.Operation != "download" {
		slog.Error("Invalid operation", "operation", claims.Operation)
		return
	}

	if claims.Filesize <= 0 || claims.Filesize > 2<<26 { //Max 64 MB
		slog.Error("Invalid file size", "file_size", claims.Filesize)
	}

	// Start background lease renewal (every 30s) to handle long transfers
	stopLeaseRenewal := make(chan struct{})
	defer close(stopLeaseRenewal)
	go fds.backgroundLeaseRenewal(claims.ChunkHandle, stopLeaseRenewal)

	ats := allocatortrackingservice.GetAllocatorTrackingService()
	currAllocator, err := ats.GetAllocator(claims.ChunkHandle)
	if err != nil {
		currAllocator = allocator.NewAllocator(64 << 20)
		ats.AddAllocator(claims.ChunkHandle, currAllocator)
	}

	opId := uuid.New().String()
	var offset uint64
	var sequence uint64

	if claims.Offset >= 0 {
		// Random write at specified offset
		offset = uint64(claims.Offset)
		sequence, err = currAllocator.AllocateAt(offset, claims.Filesize)
		if err != nil {
			slog.Error("Failed to allocate at offset", "offset", offset, "error", err)
			return
		}
		slog.Info("random write", "opID", opId, "offset", offset, "sequence", sequence)
	} else {
		// Append - allocate next offset
		offset, sequence, err = currAllocator.Allocate(claims.Filesize)
		if err != nil {
			slog.Error("Failed to allocate space for chunk", "error", err)
			return
		}
		slog.Info("append write", "opID", opId, "offset", offset, "sequence", sequence)
	}

	stagedchunk := stagedchunk.NewStagedChunk(
		claims.ChunkHandle,
		opId,
		claims.Filesize,
		offset,
		sequence,
		fds.ChunkServerConfig.Dir,
	)
	if stagedchunk == nil {
		slog.Error("failed to create staged chunk", "opId", opId)
		return
	}

	fds.ChunkStagingTrackingService.AddStagedChunk(stagedchunk)

	coordinator := fanoutcoordinator.NewFanoutCoordinator(claims.Replicas, stagedchunk)
	coordinator.SetStagedChunk(stagedchunk)
	coordinator.AddReplicas(claims.Replicas)

	// Send initial response with opID and offset
	responseBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(responseBytes, offset)
	if _, err := conn.Write(responseBytes); err != nil {
		slog.Error("Failed to send offset to client", "error", err)
		return
	}

	// Stream data from client and fanout to replicas
	slog.Info("starting fanout to replicas", "opID", opId, "numReplicas", len(claims.Replicas))
	if err := coordinator.StartFanout(conn, jwtTokenString); err != nil {
		slog.Error("Fanout failed", "error", err)
		return
	}

	slog.Info("data replication complete, waiting for quorum", "opID", opId, "chunkHandle", claims.ChunkHandle, "replicationFactor", 3)

	// Wait for quorum (2 out of 3 replicas for RF=3)
	if !fds.waitForQuorum(stagedchunk, 3, 10) {
		slog.Error("quorum not reached", "opID", opId)
		// Send failure response
		conn.Write([]byte{0}) // 0 = failure
		return
	}

	slog.Info("quorum reached, initiating commit phase", "opID", opId)

	// Renew lease before committing to ensure we're still the primary
	if mc := masterclient.GetInstance(); mc != nil {
		if duration, err := mc.RenewLease(claims.ChunkHandle); err != nil || duration == 0 {
			slog.Error("failed to renew lease before commit - no longer primary", "chunk", claims.ChunkHandle, "error", err)
			conn.Write([]byte{0}) // 0 = failure
			return
		}
	}

	// Send COMMIT to all replicas
	if err := fds.commitToReplicas(claims.Replicas, opId); err != nil {
		slog.Error("failed to commit to replicas", "error", err)
		conn.Write([]byte{0}) // 0 = failure
		return
	}

	// Commit on primary (in sequence order)
	committed, err := fds.ChunkStagingTrackingService.CommitInOrder(stagedchunk)
	if err != nil {
		slog.Error("failed to commit on primary", "error", err)
		conn.Write([]byte{0}) // 0 = failure
		return
	}
	slog.Info("primary committed", "opID", opId, "count", len(committed))

	slog.Info("commit successful", "opID", opId, "chunkHandle", claims.ChunkHandle, "offset", offset)

	// Report commit to master (if connected)
	if mc := masterclient.GetInstance(); mc != nil {
		if err := mc.ReportCommit(claims.ChunkHandle, claims.Filesize); err != nil {
			slog.Warn("failed to report commit to master", "error", err)
			// Don't fail the write - master can discover via heartbeat
		}
	}

	// Send success response to client
	conn.Write([]byte{1}) // 1 = success
}

// waitForQuorum waits for the staged chunk to reach quorum
// replicationFactor: number of total replicas (e.g., 3)
// timeoutSeconds: max time to wait
func (fds *FileDownloadService) waitForQuorum(sc *stagedchunk.StagedChunk, replicationFactor int, timeoutSeconds int) bool {
	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	quorumNeeded := replicationFactor/2 + 1

	slog.Info("waiting for quorum", "opID", sc.OpId, "quorumNeeded", quorumNeeded-1, "replicationFactor", replicationFactor)

	for time.Now().Before(deadline) {
		readyCount := sc.GetReadyCount()
		if sc.IsQuorumReady(replicationFactor) {
			slog.Info("quorum reached!", "opID", sc.OpId, "readyCount", readyCount, "quorumNeeded", quorumNeeded-1)
			return true
		}
		slog.Debug("waiting for more replicas", "opID", sc.OpId, "readyCount", readyCount, "quorumNeeded", quorumNeeded-1)
		time.Sleep(100 * time.Millisecond)
	}

	slog.Warn("timeout waiting for quorum", "opID", sc.OpId, "readyCount", sc.GetReadyCount(), "quorumNeeded", quorumNeeded-1)
	return false
}

// commitToReplicas sends COMMIT to all replicas
func (fds *FileDownloadService) commitToReplicas(replicas []csstructs.ReplicaIdentifier, opID string) error {
	errors := replicationclient.SendCommitToAllReplicas(replicas, opID)

	if len(errors) > 0 {
		return fmt.Errorf("commit failed on %d replicas", len(errors))
	}

	return nil
}

// backgroundLeaseRenewal periodically renews the lease for long-running transfers
func (fds *FileDownloadService) backgroundLeaseRenewal(chunkHandle string, stop <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Renew immediately on start
	if mc := masterclient.GetInstance(); mc != nil {
		if _, err := mc.RenewLease(chunkHandle); err != nil {
			slog.Warn("initial lease renewal failed", "chunk", chunkHandle, "error", err)
		}
	}

	for {
		select {
		case <-stop:
			slog.Debug("stopping background lease renewal", "chunk", chunkHandle)
			return
		case <-ticker.C:
			if mc := masterclient.GetInstance(); mc != nil {
				if duration, err := mc.RenewLease(chunkHandle); err != nil {
					slog.Warn("background lease renewal failed", "chunk", chunkHandle, "error", err)
				} else if duration > 0 {
					slog.Debug("background lease renewed", "chunk", chunkHandle, "duration_ms", duration)
				} else {
					slog.Warn("lost primary status during transfer", "chunk", chunkHandle)
				}
			}
		}
	}
}
