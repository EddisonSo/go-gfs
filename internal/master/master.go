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

// ChunkLocation represents where a chunk replica is stored
type ChunkLocation struct {
	ServerID        ChunkServerID
	Hostname        string
	DataPort        int
	ReplicationPort int
	LastHeartbeat   time.Time
	BuildInfo       *BuildInfo // Build version info
}

// BuildInfo contains build version information
type BuildInfo struct {
	BuildID   string
	BuildTime string
}

// ChunkStatus represents the state of a chunk
type ChunkStatus int

const (
	ChunkPending   ChunkStatus = iota // Allocated but not yet committed
	ChunkCommitted                    // Successfully written and confirmed
)

// Lease duration for primary assignment
const LeaseDuration = 60 * time.Second
const LeaseGracePeriod = 60 * time.Second // Extra time before reassigning primary
const defaultNamespace = "default"

type fileKey struct {
	Namespace string
	Path      string
}

func normalizeNamespace(namespace string) string {
	if namespace == "" {
		return defaultNamespace
	}
	return namespace
}

func makeFileKey(namespace, path string) fileKey {
	return fileKey{
		Namespace: normalizeNamespace(namespace),
		Path:      path,
	}
}

// ChunkInfo contains metadata about a chunk
type ChunkInfo struct {
	Handle          ChunkHandle
	FilePath        string          // File this chunk belongs to
	Namespace       string          // Namespace this chunk belongs to
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
	Namespace  string
	Chunks     []ChunkHandle // Ordered list of chunk handles
	Size       uint64        // Total file size in bytes
	ChunkSize  uint64        // Size of each chunk (default 64MB)
	CreatedAt  time.Time
	ModifiedAt time.Time
}

// Master is the central metadata server for GFS
type Master struct {
	// File namespace: namespace+path -> file info
	files  map[fileKey]*FileInfo
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

	// Orphaned chunk tracking: serverID:handle -> first seen time
	orphanedChunks   map[string]time.Time
	orphanedChunksMu sync.Mutex

	// Configuration
	defaultChunkSize  uint64
	replicationFactor int

	// Write-ahead log for persistence
	wal *wal.WAL
}

// NewMaster creates a new master server with WAL at the given path
func NewMaster(walPath string) (*Master, error) {
	m := &Master{
		files:             make(map[fileKey]*FileInfo),
		chunks:            make(map[ChunkHandle]*ChunkInfo),
		chunkservers:      make(map[ChunkServerID]*ChunkLocation),
		pendingDeletes:    make(map[ChunkServerID][]ChunkHandle),
		orphanedChunks:    make(map[string]time.Time),
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
			m.replayCreateFile(data.Path, data.Namespace, data.ChunkSize)

		case wal.OpDeleteFile:
			var data wal.DeleteFileData
			if err := json.Unmarshal(entry.Data, &data); err != nil {
				slog.Warn("failed to unmarshal DELETE_FILE", "error", err)
				continue
			}
			m.replayDeleteFile(data.Path, data.Namespace)

		case wal.OpDeleteNamespace:
			var data wal.DeleteNamespaceData
			if err := json.Unmarshal(entry.Data, &data); err != nil {
				slog.Warn("failed to unmarshal DELETE_NAMESPACE", "error", err)
				continue
			}
			m.replayDeleteNamespace(data.Namespace)

		case wal.OpRenameFile:
			var data wal.RenameFileData
			if err := json.Unmarshal(entry.Data, &data); err != nil {
				slog.Warn("failed to unmarshal RENAME_FILE", "error", err)
				continue
			}
			m.replayRenameFile(data.OldPath, data.NewPath, data.Namespace)

		case wal.OpAddChunk:
			var data wal.AddChunkData
			if err := json.Unmarshal(entry.Data, &data); err != nil {
				slog.Warn("failed to unmarshal ADD_CHUNK", "error", err)
				continue
			}
			m.replayAddChunk(data.Path, data.Namespace, data.ChunkHandle)

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
func (m *Master) replayCreateFile(path, namespace string, chunkSize uint64) {
	namespace = normalizeNamespace(namespace)
	now := time.Now()
	m.files[makeFileKey(namespace, path)] = &FileInfo{
		Path:       path,
		Namespace:  namespace,
		Chunks:     []ChunkHandle{},
		Size:       0,
		ChunkSize:  chunkSize,
		CreatedAt:  now,
		ModifiedAt: now,
	}
}

// replayDeleteFile deletes a file from WAL (no WAL logging)
func (m *Master) replayDeleteFile(path, namespace string) {
	key := makeFileKey(namespace, path)
	if file, exists := m.files[key]; exists {
		for _, handle := range file.Chunks {
			delete(m.chunks, handle)
		}
		delete(m.files, key)
	}
}

// replayDeleteNamespace deletes all files in a namespace from WAL (no WAL logging)
func (m *Master) replayDeleteNamespace(namespace string) {
	namespace = normalizeNamespace(namespace)
	for key, file := range m.files {
		if file.Namespace == namespace {
			for _, handle := range file.Chunks {
				delete(m.chunks, handle)
			}
			delete(m.files, key)
		}
	}
}

// replayRenameFile renames a file from WAL (no WAL logging)
func (m *Master) replayRenameFile(oldPath, newPath, namespace string) {
	key := makeFileKey(namespace, oldPath)
	if file, exists := m.files[key]; exists {
		// Update file path
		file.Path = newPath
		// Move to new key
		newKey := makeFileKey(namespace, newPath)
		m.files[newKey] = file
		delete(m.files, key)
		// Update chunk back-references
		for _, handle := range file.Chunks {
			if chunk, ok := m.chunks[handle]; ok {
				chunk.FilePath = newPath
			}
		}
	}
}

// replayAddChunk adds a chunk from WAL (no WAL logging)
func (m *Master) replayAddChunk(path, namespace, chunkHandle string) {
	handle := ChunkHandle(chunkHandle)
	key := makeFileKey(namespace, path)
	if file, exists := m.files[key]; exists {
		file.Chunks = append(file.Chunks, handle)
		m.chunks[handle] = &ChunkInfo{
			Handle:    handle,
			FilePath:  path,
			Namespace: normalizeNamespace(namespace),
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
		key := makeFileKey(chunk.Namespace, chunk.FilePath)
		if file, exists := m.files[key]; exists {
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
func (m *Master) RegisterChunkServer(id ChunkServerID, hostname string, dataPort, replicationPort int, buildInfo *BuildInfo) {
	m.csMu.Lock()
	defer m.csMu.Unlock()

	loc := &ChunkLocation{
		ServerID:        id,
		Hostname:        hostname,
		DataPort:        dataPort,
		ReplicationPort: replicationPort,
		LastHeartbeat:   time.Now(),
		BuildInfo:       buildInfo,
	}
	m.chunkservers[id] = loc
	buildID := "unknown"
	if buildInfo != nil {
		buildID = buildInfo.BuildID
	}
	slog.Info("registered chunkserver", "id", id, "hostname", hostname, "dataPort", dataPort, "build", buildID)
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

// generateChunkHandle generates a new unique chunk handle using UUIDv4
func (m *Master) generateChunkHandle() ChunkHandle {
	return ChunkHandle(uuid.New().String())
}

// CreateFile creates a new file in the namespace
func (m *Master) CreateFile(path, namespace string) (*FileInfo, error) {
	m.fileMu.Lock()
	defer m.fileMu.Unlock()

	key := makeFileKey(namespace, path)
	if _, exists := m.files[key]; exists {
		return nil, fmt.Errorf("file already exists: %s", path)
	}

	namespace = normalizeNamespace(namespace)

	// Log to WAL before applying
	if err := m.wal.LogCreateFile(path, namespace, m.defaultChunkSize); err != nil {
		return nil, fmt.Errorf("WAL write failed: %w", err)
	}

	now := time.Now()
	file := &FileInfo{
		Path:       path,
		Namespace:  namespace,
		Chunks:     []ChunkHandle{},
		Size:       0,
		ChunkSize:  m.defaultChunkSize,
		CreatedAt:  now,
		ModifiedAt: now,
	}
	m.files[key] = file

	slog.Info("created file", "path", path, "namespace", namespace)
	return file, nil
}

// GetFile returns file info for a path
func (m *Master) GetFile(path, namespace string) (*FileInfo, error) {
	m.fileMu.RLock()
	defer m.fileMu.RUnlock()

	key := makeFileKey(namespace, path)
	file, exists := m.files[key]
	if !exists {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return file, nil
}

// DeleteFile removes a file from the namespace and schedules chunk deletion
func (m *Master) DeleteFile(path, namespace string) error {
	m.fileMu.Lock()
	key := makeFileKey(namespace, path)
	file, exists := m.files[key]
	if !exists {
		m.fileMu.Unlock()
		return fmt.Errorf("file not found: %s", path)
	}

	// Log to WAL before applying
	if err := m.wal.LogDeleteFile(path, normalizeNamespace(namespace)); err != nil {
		m.fileMu.Unlock()
		return fmt.Errorf("WAL write failed: %w", err)
	}

	// Get chunk handles before deleting file
	chunkHandles := make([]ChunkHandle, len(file.Chunks))
	copy(chunkHandles, file.Chunks)

	delete(m.files, key)
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

	slog.Info("deleted file", "path", path, "namespace", normalizeNamespace(namespace), "chunks", len(chunkHandles))
	return nil
}

// DeleteNamespace removes a namespace and all its files
func (m *Master) DeleteNamespace(namespace string) (int, error) {
	namespace = normalizeNamespace(namespace)
	if namespace == defaultNamespace {
		return 0, fmt.Errorf("cannot delete default namespace")
	}

	m.fileMu.Lock()

	// Find all files in this namespace
	var filesToDelete []fileKey
	var allChunkHandles []ChunkHandle
	for key, file := range m.files {
		if file.Namespace == namespace {
			filesToDelete = append(filesToDelete, key)
			allChunkHandles = append(allChunkHandles, file.Chunks...)
		}
	}

	if len(filesToDelete) == 0 {
		m.fileMu.Unlock()
		return 0, nil
	}

	// Log to WAL
	if err := m.wal.LogDeleteNamespace(namespace); err != nil {
		m.fileMu.Unlock()
		return 0, fmt.Errorf("WAL write failed: %w", err)
	}

	// Delete all files
	for _, key := range filesToDelete {
		delete(m.files, key)
	}
	m.fileMu.Unlock()

	// Schedule chunk deletions
	m.chunkMu.Lock()
	m.pendingDeletesMu.Lock()
	for _, handle := range allChunkHandles {
		if chunk, ok := m.chunks[handle]; ok {
			for _, loc := range chunk.Locations {
				m.pendingDeletes[loc.ServerID] = append(m.pendingDeletes[loc.ServerID], handle)
			}
			delete(m.chunks, handle)
		}
	}
	m.pendingDeletesMu.Unlock()
	m.chunkMu.Unlock()

	slog.Info("deleted namespace", "namespace", namespace, "files", len(filesToDelete), "chunks", len(allChunkHandles))
	return len(filesToDelete), nil
}

// RenameFile renames/moves a file from oldPath to newPath
func (m *Master) RenameFile(oldPath, newPath, namespace string) error {
	m.fileMu.Lock()
	defer m.fileMu.Unlock()

	// Check source exists
	namespace = normalizeNamespace(namespace)
	oldKey := makeFileKey(namespace, oldPath)
	file, exists := m.files[oldKey]
	if !exists {
		return fmt.Errorf("file not found: %s", oldPath)
	}

	// Check destination doesn't exist
	newKey := makeFileKey(namespace, newPath)
	if _, exists := m.files[newKey]; exists {
		return fmt.Errorf("file already exists: %s", newPath)
	}

	// Log to WAL before applying
	if err := m.wal.LogRenameFile(oldPath, newPath, namespace); err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}

	// Update file path
	file.Path = newPath
	file.ModifiedAt = time.Now()

	// Move to new key
	m.files[newKey] = file
	delete(m.files, oldKey)

	// Update chunk back-references
	m.chunkMu.Lock()
	for _, handle := range file.Chunks {
		if chunk, ok := m.chunks[handle]; ok {
			chunk.FilePath = newPath
		}
	}
	m.chunkMu.Unlock()

	slog.Info("renamed file", "oldPath", oldPath, "newPath", newPath, "namespace", namespace)
	return nil
}

// ListFiles returns all files in the namespace
func (m *Master) ListFiles(namespace, prefix string) []*FileInfo {
	m.fileMu.RLock()
	defer m.fileMu.RUnlock()

	files := make([]*FileInfo, 0, len(m.files))
	for _, f := range m.files {
		if namespace != "" && f.Namespace != namespace {
			continue
		}
		if prefix != "" && len(f.Path) >= len(prefix) {
			if f.Path[:len(prefix)] != prefix {
				continue
			}
		}
		files = append(files, f)
	}
	return files
}

// AddChunkToFile adds a new chunk to a file and returns chunk info with replica locations
func (m *Master) AddChunkToFile(path, namespace string) (*ChunkInfo, error) {
	// Check file exists first (brief read lock)
	m.fileMu.RLock()
	key := makeFileKey(namespace, path)
	_, exists := m.files[key]
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
	if err := m.wal.LogAddChunk(path, normalizeNamespace(namespace), string(handle)); err != nil {
		return nil, fmt.Errorf("WAL write failed: %w", err)
	}

	// Now acquire locks and apply changes
	m.fileMu.Lock()
	defer m.fileMu.Unlock()

	// Re-check file still exists
	file, exists := m.files[key]
	if !exists {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	// Create chunk info with lease
	m.chunkMu.Lock()
	chunkInfo := &ChunkInfo{
		Handle:          handle,
		FilePath:        path,
		Namespace:       normalizeNamespace(namespace),
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

	slog.Info("added chunk to file", "path", path, "namespace", normalizeNamespace(namespace), "chunk", handle, "replicas", len(replicas))
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
func (m *Master) GetFileChunks(path, namespace string) ([]*ChunkInfo, error) {
	m.fileMu.RLock()
	key := makeFileKey(namespace, path)
	file, exists := m.files[key]
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
			// Only reassign primary if lease has been expired for longer than grace period
			// This prevents reassigning during in-flight operations
			if chunk.Primary != nil && now.After(chunk.LeaseExpiration.Add(LeaseGracePeriod)) {
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

	// Select first available replica as the new primary
	m.csMu.RLock()
	var bestLoc *ChunkLocation
	for i := range chunk.Locations {
		loc := &chunk.Locations[i]
		// Check if this server is still alive
		if _, ok := m.chunkservers[loc.ServerID]; ok {
			bestLoc = loc
			break
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
		slog.Warn("lease renewal failed: chunk not found", "chunk", handle, "requestor", serverID)
		return false
	}

	// Only the current primary can renew the lease
	if chunk.Primary == nil {
		slog.Warn("lease renewal failed: no primary assigned",
			"chunk", handle,
			"requestor", serverID,
			"numLocations", len(chunk.Locations))
		return false
	}

	if chunk.Primary.ServerID != serverID {
		slog.Warn("lease renewal failed: requestor is not primary",
			"chunk", handle,
			"requestor", serverID,
			"actualPrimary", chunk.Primary.ServerID,
			"leaseExpired", time.Now().After(chunk.LeaseExpiration))
		return false
	}

	chunk.LeaseExpiration = time.Now().Add(LeaseDuration)
	slog.Debug("lease renewed", "chunk", handle, "primary", serverID, "expires", chunk.LeaseExpiration)
	return true
}

// ReportChunk is called by chunkservers to report they have a chunk
func (m *Master) ReportChunk(serverID ChunkServerID, handle ChunkHandle) {
	m.csMu.RLock()
	_, exists := m.chunkservers[serverID]
	m.csMu.RUnlock()

	if !exists {
		slog.Warn("chunk report from unknown server", "serverID", serverID)
		return
	}

	m.chunkMu.RLock()
	chunk, chunkExists := m.chunks[handle]
	m.chunkMu.RUnlock()

	if !chunkExists {
		// Check if chunk is already pending deletion (recently deleted file)
		m.pendingDeletesMu.Lock()
		pendingForServer := m.pendingDeletes[serverID]
		alreadyPendingDelete := false
		for _, h := range pendingForServer {
			if h == handle {
				alreadyPendingDelete = true
				break
			}
		}
		m.pendingDeletesMu.Unlock()

		if alreadyPendingDelete {
			// Chunk is scheduled for deletion, not truly orphaned
			slog.Debug("chunk pending deletion reported by server", "serverID", serverID, "handle", handle)
			return
		}

		// Chunk not in master's records - track as orphaned
		m.handleOrphanedChunk(serverID, handle)
		return
	}

	// Clear from orphaned tracking if it was previously unknown but now exists
	orphanKey := string(serverID) + ":" + string(handle)
	m.orphanedChunksMu.Lock()
	delete(m.orphanedChunks, orphanKey)
	m.orphanedChunksMu.Unlock()

	m.chunkMu.Lock()
	defer m.chunkMu.Unlock()

	// Re-check after acquiring write lock
	chunk, exists = m.chunks[handle]
	if !exists {
		return
	}

	// Check if this server is already in the location list
	for _, loc := range chunk.Locations {
		if loc.ServerID == serverID {
			return // Already tracked
		}
	}

	// Add this server to the chunk's location list
	m.csMu.RLock()
	serverLoc := m.chunkservers[serverID]
	m.csMu.RUnlock()
	if serverLoc != nil {
		chunk.Locations = append(chunk.Locations, *serverLoc)
		slog.Info("added chunk location", "handle", handle, "serverID", serverID)

		// If chunk has no primary, assign this server as primary
		if chunk.Primary == nil {
			chunk.Primary = serverLoc
			chunk.LeaseExpiration = time.Now().Add(LeaseDuration)
			slog.Info("assigned primary from reported location", "handle", handle, "primary", serverID)
		}
	}
}

// handleOrphanedChunk tracks unknown chunks and schedules deletion after grace period
func (m *Master) handleOrphanedChunk(serverID ChunkServerID, handle ChunkHandle) {
	orphanKey := string(serverID) + ":" + string(handle)
	gracePeriod := 1 * time.Hour

	m.orphanedChunksMu.Lock()
	defer m.orphanedChunksMu.Unlock()

	firstSeen, tracked := m.orphanedChunks[orphanKey]
	if !tracked {
		// First time seeing this orphaned chunk, start tracking
		m.orphanedChunks[orphanKey] = time.Now()
		slog.Warn("orphaned chunk detected, will delete after grace period",
			"serverID", serverID, "handle", handle, "gracePeriod", gracePeriod)
		return
	}

	// Check if grace period has passed
	if time.Since(firstSeen) < gracePeriod {
		slog.Debug("orphaned chunk still in grace period",
			"serverID", serverID, "handle", handle,
			"remaining", gracePeriod-time.Since(firstSeen))
		return
	}

	// Grace period passed, schedule for deletion
	slog.Info("scheduling orphaned chunk for deletion",
		"serverID", serverID, "handle", handle,
		"orphanedFor", time.Since(firstSeen))

	m.pendingDeletesMu.Lock()
	m.pendingDeletes[serverID] = append(m.pendingDeletes[serverID], handle)
	m.pendingDeletesMu.Unlock()

	// Remove from orphan tracking
	delete(m.orphanedChunks, orphanKey)
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
	fileNamespace := chunk.Namespace
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
	if file, exists := m.files[makeFileKey(fileNamespace, filePath)]; exists {
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
