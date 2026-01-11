package master

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"eddisonso.com/go-gfs/internal/master/wal"
	"github.com/google/uuid"
)

// ChunkHandle is a unique identifier for a chunk
type ChunkHandle string

// ChunkServerID is a unique identifier for a chunkserver
type ChunkServerID string

// ResourceMetrics holds CPU, memory, and disk usage information from a chunkserver
type ResourceMetrics struct {
	CPUUsagePercent    float64
	MemoryUsedBytes    uint64
	MemoryTotalBytes   uint64
	MemoryUsagePercent float64
	DiskUsedBytes      uint64
	DiskTotalBytes     uint64
	DiskUsagePercent   float64
}

// ChunkLocation represents where a chunk replica is stored
type ChunkLocation struct {
	ServerID        ChunkServerID
	Hostname        string
	DataPort        int
	ReplicationPort int
	LastHeartbeat   time.Time
	Resources       *ResourceMetrics // Current resource usage
}

// ChunkStatus represents the state of a chunk
type ChunkStatus int

const (
	ChunkPending   ChunkStatus = iota // Allocated but not yet committed
	ChunkCommitted                    // Successfully written and confirmed
)

// Lease duration for primary assignment
const LeaseDuration = 60 * time.Second

// ChunkInfo contains metadata about a chunk
type ChunkInfo struct {
	Handle          ChunkHandle
	FilePath        string          // File this chunk belongs to
	Locations       []ChunkLocation // Replica locations
	Version         uint64          // Chunk version for consistency
	Primary         *ChunkLocation  // Current primary (lease holder)
	LeaseExpiration time.Time       // When the primary's lease expires
	Status          ChunkStatus     // Pending or Committed
	Size            uint64          // Actual size written (set on commit)
}

// FileInfo contains metadata about a file
type FileInfo struct {
	Path       string
	Chunks     []ChunkHandle // Ordered list of chunk handles
	Size       uint64        // Total file size in bytes
	ChunkSize  uint64        // Size of each chunk (default 64MB)
	CreatedAt  time.Time
	ModifiedAt time.Time
}

// Master is the central metadata server for GFS
type Master struct {
	// File namespace: path -> file info
	files  map[string]*FileInfo
	fileMu sync.RWMutex

	// Chunk metadata: handle -> chunk info
	chunks  map[ChunkHandle]*ChunkInfo
	chunkMu sync.RWMutex

	// Chunkserver registry: serverID -> location info
	chunkservers map[ChunkServerID]*ChunkLocation
	csMu         sync.RWMutex

	// Chunks pending deletion per chunkserver
	pendingDeletes   map[ChunkServerID][]ChunkHandle
	pendingDeletesMu sync.Mutex

	// Configuration
	defaultChunkSize  uint64
	replicationFactor int

	// Write-ahead log for persistence
	wal *wal.WAL
}

// NewMaster creates a new master server with WAL at the given path
func NewMaster(walPath string) (*Master, error) {
	m := &Master{
		files:             make(map[string]*FileInfo),
		chunks:            make(map[ChunkHandle]*ChunkInfo),
		chunkservers:      make(map[ChunkServerID]*ChunkLocation),
		pendingDeletes:    make(map[ChunkServerID][]ChunkHandle),
		defaultChunkSize:  64 << 20, // 64MB
		replicationFactor: 3,
	}

	// Initialize WAL
	w, err := wal.New(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}
	m.wal = w

	// Replay WAL to restore state
	if err := m.replayWAL(walPath); err != nil {
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	return m, nil
}

// replayWAL replays the WAL to restore master state
func (m *Master) replayWAL(walPath string) error {
	reader, err := wal.NewReader(walPath)
	if err != nil {
		return err
	}
	if reader == nil {
		slog.Info("no WAL to replay")
		return nil
	}
	defer reader.Close()

	entries, err := reader.ReadAll()
	if err != nil {
		return err
	}

	slog.Info("replaying WAL", "entries", len(entries))

	for _, entry := range entries {
		switch entry.Op {
		case wal.OpCreateFile:
			var data wal.CreateFileData
			if err := json.Unmarshal(entry.Data, &data); err != nil {
				slog.Warn("failed to unmarshal CREATE_FILE", "error", err)
				continue
			}
			m.replayCreateFile(data.Path, data.ChunkSize)

		case wal.OpDeleteFile:
			var data wal.DeleteFileData
			if err := json.Unmarshal(entry.Data, &data); err != nil {
				slog.Warn("failed to unmarshal DELETE_FILE", "error", err)
				continue
			}
			m.replayDeleteFile(data.Path)

		case wal.OpAddChunk:
			var data wal.AddChunkData
			if err := json.Unmarshal(entry.Data, &data); err != nil {
				slog.Warn("failed to unmarshal ADD_CHUNK", "error", err)
				continue
			}
			m.replayAddChunk(data.Path, data.ChunkHandle)

		case wal.OpCommitChunk:
			var data wal.CommitChunkData
			if err := json.Unmarshal(entry.Data, &data); err != nil {
				slog.Warn("failed to unmarshal COMMIT_CHUNK", "error", err)
				continue
			}
			m.replayCommitChunk(data.ChunkHandle, data.Size)

		case wal.OpSetCounter:
			// Legacy counter entries are ignored - we now use UUIDs for chunk handles
		}
	}

	slog.Info("WAL replay complete", "files", len(m.files), "chunks", len(m.chunks))
	return nil
}

// replayCreateFile recreates a file from WAL (no WAL logging)
func (m *Master) replayCreateFile(path string, chunkSize uint64) {
	now := time.Now()
	m.files[path] = &FileInfo{
		Path:       path,
		Chunks:     []ChunkHandle{},
		Size:       0,
		ChunkSize:  chunkSize,
		CreatedAt:  now,
		ModifiedAt: now,
	}
}

// replayDeleteFile deletes a file from WAL (no WAL logging)
func (m *Master) replayDeleteFile(path string) {
	if file, exists := m.files[path]; exists {
		for _, handle := range file.Chunks {
			delete(m.chunks, handle)
		}
		delete(m.files, path)
	}
}

// replayAddChunk adds a chunk from WAL (no WAL logging)
func (m *Master) replayAddChunk(path, chunkHandle string) {
	handle := ChunkHandle(chunkHandle)
	if file, exists := m.files[path]; exists {
		file.Chunks = append(file.Chunks, handle)
		m.chunks[handle] = &ChunkInfo{
			Handle:    handle,
			FilePath:  path,
			Locations: []ChunkLocation{},
			Version:   1,
			Status:    ChunkPending,
		}
	}
}

// replayCommitChunk commits a chunk from WAL (no WAL logging)
func (m *Master) replayCommitChunk(chunkHandle string, size uint64) {
	handle := ChunkHandle(chunkHandle)
	if chunk, exists := m.chunks[handle]; exists {
		chunk.Status = ChunkCommitted
		chunk.Size = size
		// Update file size
		if file, exists := m.files[chunk.FilePath]; exists {
			var totalSize uint64
			for _, h := range file.Chunks {
				if c, ok := m.chunks[h]; ok {
					totalSize += c.Size
				}
			}
			file.Size = totalSize
		}
	}
}

// Close closes the master and its WAL
func (m *Master) Close() error {
	if m.wal != nil {
		return m.wal.Close()
	}
	return nil
}

// RegisterChunkServer registers a chunkserver with the master
func (m *Master) RegisterChunkServer(id ChunkServerID, hostname string, dataPort, replicationPort int) {
	m.csMu.Lock()
	defer m.csMu.Unlock()

	loc := &ChunkLocation{
		ServerID:        id,
		Hostname:        hostname,
		DataPort:        dataPort,
		ReplicationPort: replicationPort,
		LastHeartbeat:   time.Now(),
	}
	m.chunkservers[id] = loc
	slog.Info("registered chunkserver", "id", id, "hostname", hostname, "dataPort", dataPort)
}

// Heartbeat updates the last heartbeat time and resource metrics for a chunkserver
func (m *Master) Heartbeat(id ChunkServerID, resources *ResourceMetrics) bool {
	m.csMu.Lock()
	defer m.csMu.Unlock()

	if loc, ok := m.chunkservers[id]; ok {
		loc.LastHeartbeat = time.Now()
		loc.Resources = resources
		return true
	}
	return false
}

// GetPendingDeletes returns and clears the list of chunks to delete for a chunkserver
func (m *Master) GetPendingDeletes(id ChunkServerID) []ChunkHandle {
	m.pendingDeletesMu.Lock()
	defer m.pendingDeletesMu.Unlock()

	chunks := m.pendingDeletes[id]
	delete(m.pendingDeletes, id)
	return chunks
}

// GetChunkServers returns all registered chunkservers
func (m *Master) GetChunkServers() []*ChunkLocation {
	m.csMu.RLock()
	defer m.csMu.RUnlock()

	servers := make([]*ChunkLocation, 0, len(m.chunkservers))
	for _, loc := range m.chunkservers {
		servers = append(servers, loc)
	}
	return servers
}

// GetChunkServerResources returns the resource metrics for a specific chunkserver
func (m *Master) GetChunkServerResources(id ChunkServerID) *ResourceMetrics {
	m.csMu.RLock()
	defer m.csMu.RUnlock()

	if loc, ok := m.chunkservers[id]; ok {
		return loc.Resources
	}
	return nil
}

// ChunkServerStatus contains full status information for a chunkserver
type ChunkServerStatus struct {
	Location   *ChunkLocation
	ChunkCount int
	IsAlive    bool
}

// GetClusterStatus returns status information for all chunkservers
func (m *Master) GetClusterStatus() []ChunkServerStatus {
	m.csMu.RLock()
	defer m.csMu.RUnlock()

	// Count chunks per server
	m.chunkMu.RLock()
	chunkCounts := make(map[ChunkServerID]int)
	for _, chunk := range m.chunks {
		for _, loc := range chunk.Locations {
			chunkCounts[loc.ServerID]++
		}
	}
	m.chunkMu.RUnlock()

	// Build status list
	statuses := make([]ChunkServerStatus, 0, len(m.chunkservers))
	now := time.Now()
	heartbeatTimeout := 30 * time.Second // Consider server dead if no heartbeat in 30s

	for id, loc := range m.chunkservers {
		status := ChunkServerStatus{
			Location:   loc,
			ChunkCount: chunkCounts[id],
			IsAlive:    now.Sub(loc.LastHeartbeat) < heartbeatTimeout,
		}
		statuses = append(statuses, status)
	}

	return statuses
}

// GetLeastLoadedServers returns chunkservers sorted by combined resource pressure
// Lower scores indicate less resource pressure (better candidates for placement)
func (m *Master) GetLeastLoadedServers(n int) []*ChunkLocation {
	m.csMu.RLock()
	defer m.csMu.RUnlock()

	type serverScore struct {
		loc   *ChunkLocation
		score float64
	}

	scores := make([]serverScore, 0, len(m.chunkservers))
	for _, loc := range m.chunkservers {
		score := 0.0
		if loc.Resources != nil {
			// Weight CPU and memory equally, disk slightly less
			// (since disk is typically larger and less critical short-term)
			score = loc.Resources.CPUUsagePercent*0.4 +
				loc.Resources.MemoryUsagePercent*0.4 +
				loc.Resources.DiskUsagePercent*0.2
		}
		scores = append(scores, serverScore{loc: loc, score: score})
	}

	// Sort by score (lowest first)
	for i := 0; i < len(scores)-1; i++ {
		for j := i + 1; j < len(scores); j++ {
			if scores[j].score < scores[i].score {
				scores[i], scores[j] = scores[j], scores[i]
			}
		}
	}

	result := make([]*ChunkLocation, 0, n)
	for i := 0; i < len(scores) && i < n; i++ {
		result = append(result, scores[i].loc)
	}
	return result
}

// generateChunkHandle generates a new unique chunk handle using UUIDv4
func (m *Master) generateChunkHandle() ChunkHandle {
	return ChunkHandle(uuid.New().String())
}

// CreateFile creates a new file in the namespace
func (m *Master) CreateFile(path string) (*FileInfo, error) {
	m.fileMu.Lock()
	defer m.fileMu.Unlock()

	if _, exists := m.files[path]; exists {
		return nil, fmt.Errorf("file already exists: %s", path)
	}

	// Log to WAL before applying
	if err := m.wal.LogCreateFile(path, m.defaultChunkSize); err != nil {
		return nil, fmt.Errorf("WAL write failed: %w", err)
	}

	now := time.Now()
	file := &FileInfo{
		Path:       path,
		Chunks:     []ChunkHandle{},
		Size:       0,
		ChunkSize:  m.defaultChunkSize,
		CreatedAt:  now,
		ModifiedAt: now,
	}
	m.files[path] = file

	slog.Info("created file", "path", path)
	return file, nil
}

// GetFile returns file info for a path
func (m *Master) GetFile(path string) (*FileInfo, error) {
	m.fileMu.RLock()
	defer m.fileMu.RUnlock()

	file, exists := m.files[path]
	if !exists {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return file, nil
}

// DeleteFile removes a file from the namespace and schedules chunk deletion
func (m *Master) DeleteFile(path string) error {
	m.fileMu.Lock()
	file, exists := m.files[path]
	if !exists {
		m.fileMu.Unlock()
		return fmt.Errorf("file not found: %s", path)
	}

	// Log to WAL before applying
	if err := m.wal.LogDeleteFile(path); err != nil {
		m.fileMu.Unlock()
		return fmt.Errorf("WAL write failed: %w", err)
	}

	// Get chunk handles before deleting file
	chunkHandles := make([]ChunkHandle, len(file.Chunks))
	copy(chunkHandles, file.Chunks)

	delete(m.files, path)
	m.fileMu.Unlock()

	// Remove chunks from registry and schedule deletion on chunkservers
	m.chunkMu.Lock()
	m.pendingDeletesMu.Lock()
	for _, handle := range chunkHandles {
		if chunk, ok := m.chunks[handle]; ok {
			// Schedule deletion on each chunkserver that has this chunk
			for _, loc := range chunk.Locations {
				m.pendingDeletes[loc.ServerID] = append(m.pendingDeletes[loc.ServerID], handle)
			}
			delete(m.chunks, handle)
		}
	}
	m.pendingDeletesMu.Unlock()
	m.chunkMu.Unlock()

	slog.Info("deleted file", "path", path, "chunks", len(chunkHandles))
	return nil
}

// ListFiles returns all files in the namespace
func (m *Master) ListFiles() []*FileInfo {
	m.fileMu.RLock()
	defer m.fileMu.RUnlock()

	files := make([]*FileInfo, 0, len(m.files))
	for _, f := range m.files {
		files = append(files, f)
	}
	return files
}

// AddChunkToFile adds a new chunk to a file and returns chunk info with replica locations
func (m *Master) AddChunkToFile(path string) (*ChunkInfo, error) {
	// Check file exists first (brief read lock)
	m.fileMu.RLock()
	_, exists := m.files[path]
	m.fileMu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	// Generate chunk handle (thread-safe, no lock needed)
	handle := m.generateChunkHandle()

	// Select replicas BEFORE taking fileMu to avoid lock contention
	replicas := m.selectReplicas(m.replicationFactor)
	if len(replicas) < m.replicationFactor {
		slog.Warn("insufficient chunkservers for full replication",
			"available", len(replicas),
			"required", m.replicationFactor)
	}
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no chunkservers available")
	}

	// Log to WAL OUTSIDE of locks to avoid blocking
	if err := m.wal.LogAddChunk(path, string(handle)); err != nil {
		return nil, fmt.Errorf("WAL write failed: %w", err)
	}

	// Now acquire locks and apply changes
	m.fileMu.Lock()
	defer m.fileMu.Unlock()

	// Re-check file still exists
	file, exists := m.files[path]
	if !exists {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	// Create chunk info with lease
	m.chunkMu.Lock()
	chunkInfo := &ChunkInfo{
		Handle:          handle,
		FilePath:        path,
		Locations:       replicas,
		Version:         1,
		Primary:         &replicas[0], // First replica is primary
		LeaseExpiration: time.Now().Add(LeaseDuration),
	}
	m.chunks[handle] = chunkInfo
	m.chunkMu.Unlock()

	// Add chunk to file
	file.Chunks = append(file.Chunks, handle)
	file.ModifiedAt = time.Now()

	slog.Info("added chunk to file", "path", path, "chunk", handle, "replicas", len(replicas))
	return chunkInfo, nil
}

// selectReplicas selects n chunkservers to hold replicas
func (m *Master) selectReplicas(n int) []ChunkLocation {
	m.csMu.RLock()
	defer m.csMu.RUnlock()

	// Simple selection: just pick first n available servers
	// TODO: implement smarter placement (rack awareness, load balancing)
	replicas := make([]ChunkLocation, 0, n)
	for _, loc := range m.chunkservers {
		if len(replicas) >= n {
			break
		}
		replicas = append(replicas, *loc)
	}
	return replicas
}

// GetChunkInfo returns info about a specific chunk
func (m *Master) GetChunkInfo(handle ChunkHandle) (*ChunkInfo, error) {
	m.chunkMu.RLock()
	defer m.chunkMu.RUnlock()

	chunk, exists := m.chunks[handle]
	if !exists {
		return nil, fmt.Errorf("chunk not found: %s", handle)
	}
	return chunk, nil
}

// GetFileChunks returns all chunk info for a file, checking and renewing leases as needed
func (m *Master) GetFileChunks(path string) ([]*ChunkInfo, error) {
	m.fileMu.RLock()
	file, exists := m.files[path]
	m.fileMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	m.chunkMu.Lock()
	defer m.chunkMu.Unlock()

	now := time.Now()
	chunks := make([]*ChunkInfo, 0, len(file.Chunks))
	for _, handle := range file.Chunks {
		if chunk, ok := m.chunks[handle]; ok {
			// Check if lease has expired and reassign primary if needed
			if chunk.Primary != nil && now.After(chunk.LeaseExpiration) {
				m.reassignPrimaryLocked(chunk)
			}
			chunks = append(chunks, chunk)
		}
	}
	return chunks, nil
}

// reassignPrimaryLocked selects a new primary from available replicas
// Must be called with chunkMu held
func (m *Master) reassignPrimaryLocked(chunk *ChunkInfo) {
	if len(chunk.Locations) == 0 {
		chunk.Primary = nil
		return
	}

	// Select the least loaded replica as the new primary
	m.csMu.RLock()
	var bestLoc *ChunkLocation
	var bestScore float64 = 101 // Higher than any possible score

	for i := range chunk.Locations {
		loc := &chunk.Locations[i]
		// Check if this server is still alive
		if cs, ok := m.chunkservers[loc.ServerID]; ok {
			// Calculate load score (lower is better)
			score := 50.0 // Default score if no resource data
			if cs.Resources != nil {
				score = cs.Resources.CPUUsagePercent*0.4 +
					cs.Resources.MemoryUsagePercent*0.4 +
					cs.Resources.DiskUsagePercent*0.2
			}
			if score < bestScore {
				bestScore = score
				bestLoc = loc
			}
		}
	}
	m.csMu.RUnlock()

	if bestLoc != nil {
		oldPrimary := ""
		if chunk.Primary != nil {
			oldPrimary = string(chunk.Primary.ServerID)
		}
		chunk.Primary = bestLoc
		chunk.LeaseExpiration = time.Now().Add(LeaseDuration)
		slog.Info("reassigned primary (lease expired)",
			"chunk", chunk.Handle,
			"oldPrimary", oldPrimary,
			"newPrimary", bestLoc.ServerID,
			"leaseExpires", chunk.LeaseExpiration)
	} else {
		chunk.Primary = nil
		slog.Warn("no available replicas for primary assignment", "chunk", chunk.Handle)
	}
}

// RenewLease extends the lease for a chunk's primary
// Returns true if the lease was renewed, false if the server is not the current primary
func (m *Master) RenewLease(handle ChunkHandle, serverID ChunkServerID) bool {
	m.chunkMu.Lock()
	defer m.chunkMu.Unlock()

	chunk, exists := m.chunks[handle]
	if !exists {
		return false
	}

	// Only the current primary can renew the lease
	if chunk.Primary == nil || chunk.Primary.ServerID != serverID {
		return false
	}

	chunk.LeaseExpiration = time.Now().Add(LeaseDuration)
	slog.Debug("lease renewed", "chunk", handle, "primary", serverID, "expires", chunk.LeaseExpiration)
	return true
}

// ReportChunk is called by chunkservers to report they have a chunk
func (m *Master) ReportChunk(serverID ChunkServerID, handle ChunkHandle) {
	m.csMu.RLock()
	serverLoc, exists := m.chunkservers[serverID]
	m.csMu.RUnlock()

	if !exists {
		slog.Warn("chunk report from unknown server", "serverID", serverID)
		return
	}

	m.chunkMu.Lock()
	defer m.chunkMu.Unlock()

	chunk, exists := m.chunks[handle]
	if !exists {
		// Chunk not in master's records - could be stale or orphaned
		slog.Warn("chunk report for unknown chunk", "serverID", serverID, "handle", handle)
		return
	}

	// Check if this server is already in the location list
	for _, loc := range chunk.Locations {
		if loc.ServerID == serverID {
			return // Already tracked
		}
	}

	// Add this server to the chunk's location list
	chunk.Locations = append(chunk.Locations, *serverLoc)
	slog.Info("added chunk location", "handle", handle, "serverID", serverID)
}

// ConfirmChunkCommit is called by chunkserver after successful 2PC commit
func (m *Master) ConfirmChunkCommit(serverID ChunkServerID, handle ChunkHandle, size uint64) error {
	// Check chunk exists first (brief lock)
	m.chunkMu.RLock()
	chunk, exists := m.chunks[handle]
	if !exists {
		m.chunkMu.RUnlock()
		return fmt.Errorf("chunk not found: %s", handle)
	}
	filePath := chunk.FilePath
	m.chunkMu.RUnlock()

	// Log to WAL OUTSIDE of locks to avoid blocking other operations
	if err := m.wal.LogCommitChunk(string(handle), size); err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}

	// Now update chunk status (brief lock)
	m.chunkMu.Lock()
	// Re-check in case chunk was deleted
	chunk, exists = m.chunks[handle]
	if exists {
		chunk.Status = ChunkCommitted
		chunk.Size = size
	}
	m.chunkMu.Unlock()

	// Update file size
	m.fileMu.Lock()
	if file, exists := m.files[filePath]; exists {
		// Recalculate total file size from all chunks
		m.chunkMu.RLock()
		var totalSize uint64
		for _, h := range file.Chunks {
			if c, ok := m.chunks[h]; ok {
				totalSize += c.Size
			}
		}
		m.chunkMu.RUnlock()
		file.Size = totalSize
	}
	m.fileMu.Unlock()

	slog.Info("chunk commit confirmed", "handle", handle, "serverID", serverID, "size", size, "status", "committed")
	return nil
}
