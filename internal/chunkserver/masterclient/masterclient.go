package masterclient

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "eddisonso.com/go-gfs/gen/master"
	"eddisonso.com/go-gfs/internal/buildinfo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Singleton instance for accessing master client from other packages
var (
	instance *MasterClient
	once     sync.Once
)

// SetInstance sets the singleton instance (called from main)
func SetInstance(mc *MasterClient) {
	once.Do(func() {
		instance = mc
	})
}

// GetInstance returns the singleton instance (may be nil if not connected to master)
func GetInstance() *MasterClient {
	return instance
}

// MasterClient handles communication with the master server
type MasterClient struct {
	serverID        string
	hostname        string
	dataPort        int
	replicationPort int
	storageDir      string
	masterAddr      string

	conn   *grpc.ClientConn
	client pb.MasterClient

	// Heartbeat control
	stopHeartbeat chan struct{}
	wg            sync.WaitGroup
}

// NewMasterClient creates a new master client
func NewMasterClient(serverID, hostname string, dataPort, replicationPort int, storageDir, masterAddr string) *MasterClient {
	return &MasterClient{
		serverID:        serverID,
		hostname:        hostname,
		dataPort:        dataPort,
		replicationPort: replicationPort,
		storageDir:      storageDir,
		masterAddr:      masterAddr,
		stopHeartbeat:   make(chan struct{}),
	}
}

// Connect establishes connection to the master
func (mc *MasterClient) Connect() error {
	conn, err := grpc.NewClient(mc.masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	mc.conn = conn
	mc.client = pb.NewMasterClient(conn)
	slog.Info("connected to master", "addr", mc.masterAddr)
	return nil
}

// Close closes the connection to the master
func (mc *MasterClient) Close() {
	close(mc.stopHeartbeat)
	mc.wg.Wait()
	if mc.conn != nil {
		mc.conn.Close()
	}
}

// Register registers this chunkserver with the master
func (mc *MasterClient) Register() error {
	// Scan storage directory for existing chunks
	chunks := mc.scanChunks()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := mc.client.Register(ctx, &pb.RegisterRequest{
		ServerId:        mc.serverID,
		Hostname:        mc.hostname,
		DataPort:        int32(mc.dataPort),
		ReplicationPort: int32(mc.replicationPort),
		ChunkHandles:    chunks,
		BuildInfo: &pb.BuildInfo{
			BuildId:   buildinfo.BuildID,
			BuildTime: buildinfo.BuildTime,
		},
	})

	if err != nil {
		return err
	}

	if !resp.Success {
		slog.Error("registration failed", "message", resp.Message)
	} else {
		slog.Info("registered with master", "serverID", mc.serverID, "chunks", len(chunks), "build", buildinfo.BuildID)
	}

	return nil
}

// StartHeartbeat starts the heartbeat loop in a goroutine
func (mc *MasterClient) StartHeartbeat(interval time.Duration) {
	mc.wg.Add(1)
	go func() {
		defer mc.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-mc.stopHeartbeat:
				slog.Info("stopping heartbeat")
				return
			case <-ticker.C:
				mc.sendHeartbeat()
			}
		}
	}()
	slog.Info("started heartbeat", "interval", interval)
}

// sendHeartbeat sends a single heartbeat to the master
func (mc *MasterClient) sendHeartbeat() {
	chunks := mc.scanChunks()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := mc.client.Heartbeat(ctx, &pb.HeartbeatRequest{
		ServerId:     mc.serverID,
		ChunkHandles: chunks,
	})

	if err != nil {
		slog.Error("heartbeat failed", "error", err)
		// Try to re-register in case master restarted
		mc.tryReregister()
		return
	}

	if !resp.Success {
		slog.Warn("heartbeat rejected by master, attempting re-registration")
		mc.tryReregister()
		return
	}

	// Handle any chunks master wants us to delete
	if len(resp.ChunksToDelete) > 0 {
		slog.Info("master requested chunk deletion", "count", len(resp.ChunksToDelete))
		for _, chunk := range resp.ChunksToDelete {
			mc.deleteChunk(chunk)
		}
	}

	slog.Debug("heartbeat sent", "chunks", len(chunks))
}

// tryReregister attempts to re-register with the master
func (mc *MasterClient) tryReregister() {
	if err := mc.Register(); err != nil {
		slog.Error("re-registration failed", "error", err)
	} else {
		slog.Info("re-registered with master after connection loss")
	}
}

// scanChunks scans the storage directory for chunk files
func (mc *MasterClient) scanChunks() []string {
	var chunks []string

	entries, err := os.ReadDir(mc.storageDir)
	if err != nil {
		if os.IsNotExist(err) {
			return chunks
		}
		slog.Error("failed to scan storage directory", "dir", mc.storageDir, "error", err)
		return chunks
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			chunks = append(chunks, entry.Name())
		}
	}

	return chunks
}

// deleteChunk deletes a chunk file (garbage collection)
func (mc *MasterClient) deleteChunk(chunkHandle string) {
	path := filepath.Join(mc.storageDir, chunkHandle)
	if err := os.Remove(path); err != nil {
		slog.Error("failed to delete chunk", "chunk", chunkHandle, "error", err)
	} else {
		slog.Info("deleted chunk", "chunk", chunkHandle)
	}
}

// ReportCommit notifies the master that a chunk was successfully committed
func (mc *MasterClient) ReportCommit(chunkHandle string, size uint64) error {
	if mc.client == nil {
		slog.Debug("no master connection, skipping commit report")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := mc.client.ReportCommit(ctx, &pb.ReportCommitRequest{
		ServerId:    mc.serverID,
		ChunkHandle: chunkHandle,
		Size:        size,
	})

	if err != nil {
		slog.Error("failed to report commit to master", "chunk", chunkHandle, "error", err)
		return err
	}

	if !resp.Success {
		slog.Warn("master rejected commit report", "chunk", chunkHandle, "message", resp.Message)
	} else {
		slog.Info("reported commit to master", "chunk", chunkHandle, "size", size)
	}

	return nil
}

// RenewLease requests a lease extension for a chunk this server is primary for
// Returns the lease duration in milliseconds, or 0 if renewal failed
func (mc *MasterClient) RenewLease(chunkHandle string) (uint64, error) {
	if mc.client == nil {
		slog.Debug("no master connection, skipping lease renewal")
		return 0, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := mc.client.RenewLease(ctx, &pb.RenewLeaseRequest{
		ServerId:    mc.serverID,
		ChunkHandle: chunkHandle,
	})

	if err != nil {
		slog.Error("failed to renew lease", "chunk", chunkHandle, "error", err)
		return 0, err
	}

	if !resp.Success {
		slog.Warn("lease renewal rejected", "chunk", chunkHandle, "message", resp.Message)
		return 0, nil
	}

	slog.Debug("lease renewed", "chunk", chunkHandle, "duration_ms", resp.LeaseDurationMs)
	return resp.LeaseDurationMs, nil
}

// ClaimPrimary claims primary status for a chunk at the start of a write operation.
// This should be called before accepting data from the client.
// Returns the lease duration in milliseconds, or error if claim failed.
func (mc *MasterClient) ClaimPrimary(chunkHandle string) (uint64, error) {
	if mc.client == nil {
		return 0, fmt.Errorf("no master connection")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := mc.client.ClaimPrimary(ctx, &pb.ClaimPrimaryRequest{
		ServerId:    mc.serverID,
		ChunkHandle: chunkHandle,
	})

	if err != nil {
		return 0, fmt.Errorf("claim primary RPC failed: %w", err)
	}

	if !resp.Success {
		return 0, fmt.Errorf("claim primary rejected: %s", resp.Message)
	}

	slog.Info("claimed primary", "chunk", chunkHandle, "duration_ms", resp.LeaseDurationMs)
	return resp.LeaseDurationMs, nil
}
