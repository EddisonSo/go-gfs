package master

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ChunkHandle is a unique identifier for a chunk
type ChunkHandle string

// ChunkServerID is a unique identifier for a chunkserver
type ChunkServerID string

// ChunkLocation represents where a chunk replica is stored
type ChunkLocation struct {
	ServerID        ChunkServerID
	Hostname        string
	DataPort        int
	ReplicationPort int
	LastHeartbeat   time.Time
}

// ChunkStatus represents the state of a chunk
type ChunkStatus int

const (
	ChunkPending   ChunkStatus = iota // Allocated but not yet committed
	ChunkCommitted                    // Successfully written and confirmed
)

// ChunkInfo contains metadata about a chunk
type ChunkInfo struct {
	Handle    ChunkHandle
	Locations []ChunkLocation // Replica locations
	Version   uint64          // Chunk version for consistency
	Primary   *ChunkLocation  // Current primary (lease holder)
	Status    ChunkStatus     // Pending or Committed
	Size      uint64          // Actual size written (set on commit)
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

	// Configuration
	defaultChunkSize  uint64
	replicationFactor int

	// Chunk handle counter for generating new handles
	nextChunkHandle uint64
	handleMu        sync.Mutex
}

// NewMaster creates a new master server
func NewMaster() *Master {
	return &Master{
		files:             make(map[string]*FileInfo),
		chunks:            make(map[ChunkHandle]*ChunkInfo),
		chunkservers:      make(map[ChunkServerID]*ChunkLocation),
		defaultChunkSize:  64 << 20, // 64MB
		replicationFactor: 3,
		nextChunkHandle:   1,
	}
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

// Heartbeat updates the last heartbeat time for a chunkserver
func (m *Master) Heartbeat(id ChunkServerID) bool {
	m.csMu.Lock()
	defer m.csMu.Unlock()

	if loc, ok := m.chunkservers[id]; ok {
		loc.LastHeartbeat = time.Now()
		return true
	}
	return false
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

// generateChunkHandle generates a new unique chunk handle
func (m *Master) generateChunkHandle() ChunkHandle {
	m.handleMu.Lock()
	defer m.handleMu.Unlock()

	handle := ChunkHandle(fmt.Sprintf("chunk_%d", m.nextChunkHandle))
	m.nextChunkHandle++
	return handle
}

// CreateFile creates a new file in the namespace
func (m *Master) CreateFile(path string) (*FileInfo, error) {
	m.fileMu.Lock()
	defer m.fileMu.Unlock()

	if _, exists := m.files[path]; exists {
		return nil, fmt.Errorf("file already exists: %s", path)
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

// DeleteFile removes a file from the namespace
func (m *Master) DeleteFile(path string) error {
	m.fileMu.Lock()
	defer m.fileMu.Unlock()

	if _, exists := m.files[path]; !exists {
		return fmt.Errorf("file not found: %s", path)
	}

	delete(m.files, path)
	slog.Info("deleted file", "path", path)
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
	m.fileMu.Lock()
	defer m.fileMu.Unlock()

	file, exists := m.files[path]
	if !exists {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	// Generate new chunk handle
	handle := m.generateChunkHandle()

	// Select replicas for this chunk
	replicas := m.selectReplicas(m.replicationFactor)
	if len(replicas) < m.replicationFactor {
		slog.Warn("insufficient chunkservers for full replication",
			"available", len(replicas),
			"required", m.replicationFactor)
	}

	if len(replicas) == 0 {
		return nil, fmt.Errorf("no chunkservers available")
	}

	// Create chunk info
	m.chunkMu.Lock()
	chunkInfo := &ChunkInfo{
		Handle:    handle,
		Locations: replicas,
		Version:   1,
		Primary:   &replicas[0], // First replica is primary
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

// GetFileChunks returns all chunk info for a file
func (m *Master) GetFileChunks(path string) ([]*ChunkInfo, error) {
	m.fileMu.RLock()
	file, exists := m.files[path]
	m.fileMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	m.chunkMu.RLock()
	defer m.chunkMu.RUnlock()

	chunks := make([]*ChunkInfo, 0, len(file.Chunks))
	for _, handle := range file.Chunks {
		if chunk, ok := m.chunks[handle]; ok {
			chunks = append(chunks, chunk)
		}
	}
	return chunks, nil
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
	m.chunkMu.Lock()
	defer m.chunkMu.Unlock()

	chunk, exists := m.chunks[handle]
	if !exists {
		return fmt.Errorf("chunk not found: %s", handle)
	}

	// Update chunk status
	chunk.Status = ChunkCommitted
	chunk.Size = size

	slog.Info("chunk commit confirmed", "handle", handle, "serverID", serverID, "size", size, "status", "committed")
	return nil
}
