package masterclient

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
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

// cpuStats holds CPU timing info for calculating usage
type cpuStats struct {
	idle  uint64
	total uint64
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

	// CPU stats tracking for usage calculation
	prevCPUStats *cpuStats
	cpuStatsMu   sync.Mutex
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
	resources := mc.collectResourceMetrics()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := mc.client.Heartbeat(ctx, &pb.HeartbeatRequest{
		ServerId:     mc.serverID,
		ChunkHandles: chunks,
		Resources:    resources,
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

	slog.Debug("heartbeat sent", "chunks", len(chunks),
		"cpu_percent", resources.CpuUsagePercent,
		"mem_percent", resources.MemoryUsagePercent,
		"disk_percent", resources.DiskUsagePercent)
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

// collectResourceMetrics gathers CPU, memory, and disk usage information
func (mc *MasterClient) collectResourceMetrics() *pb.ResourceMetrics {
	metrics := &pb.ResourceMetrics{}

	// Collect CPU usage
	metrics.CpuUsagePercent = mc.getCPUUsage()

	// Collect memory usage
	memUsed, memTotal, memPercent := mc.getMemoryUsage()
	metrics.MemoryUsedBytes = memUsed
	metrics.MemoryTotalBytes = memTotal
	metrics.MemoryUsagePercent = memPercent

	// Collect disk usage for storage directory
	diskUsed, diskTotal, diskPercent := mc.getDiskUsage()
	metrics.DiskUsedBytes = diskUsed
	metrics.DiskTotalBytes = diskTotal
	metrics.DiskUsagePercent = diskPercent

	return metrics
}

// getCPUUsage calculates CPU usage percentage by comparing /proc/stat readings
func (mc *MasterClient) getCPUUsage() float64 {
	file, err := os.Open("/proc/stat")
	if err != nil {
		slog.Debug("failed to open /proc/stat", "error", err)
		return 0
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)
			if len(fields) < 5 {
				return 0
			}

			// Parse CPU times: user, nice, system, idle, iowait, irq, softirq, steal
			var total, idle uint64
			for i := 1; i < len(fields); i++ {
				val, err := strconv.ParseUint(fields[i], 10, 64)
				if err != nil {
					continue
				}
				total += val
				if i == 4 { // idle is the 4th value (index 4 after "cpu")
					idle = val
				}
			}

			mc.cpuStatsMu.Lock()
			defer mc.cpuStatsMu.Unlock()

			if mc.prevCPUStats == nil {
				mc.prevCPUStats = &cpuStats{idle: idle, total: total}
				return 0 // No previous reading to compare
			}

			// Calculate CPU usage since last check
			idleDelta := idle - mc.prevCPUStats.idle
			totalDelta := total - mc.prevCPUStats.total

			mc.prevCPUStats.idle = idle
			mc.prevCPUStats.total = total

			if totalDelta == 0 {
				return 0
			}

			usage := 100.0 * float64(totalDelta-idleDelta) / float64(totalDelta)
			return usage
		}
	}

	return 0
}

// getMemoryUsage returns memory used, total, and percentage from /proc/meminfo
func (mc *MasterClient) getMemoryUsage() (used, total uint64, percent float64) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		slog.Debug("failed to open /proc/meminfo", "error", err)
		return 0, 0, 0
	}
	defer file.Close()

	var memTotal, memAvailable uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		val, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}

		switch fields[0] {
		case "MemTotal:":
			memTotal = val * 1024 // Convert from kB to bytes
		case "MemAvailable:":
			memAvailable = val * 1024
		}
	}

	if memTotal == 0 {
		return 0, 0, 0
	}

	memUsed := memTotal - memAvailable
	memPercent := 100.0 * float64(memUsed) / float64(memTotal)

	return memUsed, memTotal, memPercent
}

// getDiskUsage returns disk used, total, and percentage for the storage directory
func (mc *MasterClient) getDiskUsage() (used, total uint64, percent float64) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(mc.storageDir, &stat); err != nil {
		slog.Debug("failed to get disk stats", "dir", mc.storageDir, "error", err)
		return 0, 0, 0
	}

	total = stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	used = total - free

	if total == 0 {
		return 0, 0, 0
	}

	percent = 100.0 * float64(used) / float64(total)
	return used, total, percent
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
