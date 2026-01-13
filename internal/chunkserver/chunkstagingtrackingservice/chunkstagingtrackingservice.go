package chunkstagingtrackingservice

import (
	"log/slog"
	"sync"
	"time"

	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
)

// StagedChunkTTL is how long a staged chunk can remain uncommitted before expiring
const StagedChunkTTL = 5 * time.Minute

// CleanupInterval is how often to check for expired staged chunks
const CleanupInterval = 30 * time.Second

type ChunkStagingTrackingService struct {
	stagedChunks map[string]*stagedchunk.StagedChunk // opId -> staged chunk
	// Per-chunk commit ordering
	nextExpectedSeq map[string]uint64                            // chunkHandle -> next expected sequence
	pendingCommits  map[string]map[uint64]*stagedchunk.StagedChunk // chunkHandle -> sequence -> staged chunk
	mux             sync.Mutex
}

var (
	instance *ChunkStagingTrackingService
	once     sync.Once
)

// GetChunkStagingTrackingService returns the singleton instance
func GetChunkStagingTrackingService() *ChunkStagingTrackingService {
	once.Do(func() {
		instance = &ChunkStagingTrackingService{
			stagedChunks:    make(map[string]*stagedchunk.StagedChunk),
			nextExpectedSeq: make(map[string]uint64),
			pendingCommits:  make(map[string]map[uint64]*stagedchunk.StagedChunk),
		}
		// Start background cleanup goroutine
		go instance.cleanupExpiredChunks()
	})
	return instance
}

// cleanupExpiredChunks periodically removes staged chunks that have exceeded TTL
func (csts *ChunkStagingTrackingService) cleanupExpiredChunks() {
	ticker := time.NewTicker(CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		csts.expireOldChunks()
	}
}

// expireOldChunks removes staged chunks older than StagedChunkTTL
func (csts *ChunkStagingTrackingService) expireOldChunks() {
	csts.mux.Lock()
	defer csts.mux.Unlock()

	now := time.Now()
	expired := 0

	// Check main staged chunks
	for opId, sc := range csts.stagedChunks {
		age := now.Sub(sc.CreatedAt)
		if age > StagedChunkTTL {
			slog.Warn("staged chunk expired",
				"opId", opId,
				"chunkHandle", sc.ChunkHandle,
				"age", age.Round(time.Second),
				"ttl", StagedChunkTTL,
				"size", sc.Cap(),
				"offset", sc.Offset,
				"sequence", sc.Sequence,
			)
			sc.Close()
			delete(csts.stagedChunks, opId)
			expired++
		}
	}

	// Check pending commits
	for chunkHandle, pending := range csts.pendingCommits {
		for seq, sc := range pending {
			age := now.Sub(sc.CreatedAt)
			if age > StagedChunkTTL {
				slog.Warn("pending commit expired",
					"opId", sc.OpId,
					"chunkHandle", chunkHandle,
					"sequence", seq,
					"age", age.Round(time.Second),
					"ttl", StagedChunkTTL,
				)
				sc.Close()
				delete(pending, seq)
				expired++
			}
		}
		// Clean up empty maps
		if len(pending) == 0 {
			delete(csts.pendingCommits, chunkHandle)
		}
	}

	if expired > 0 {
		slog.Info("expired staged chunks cleanup complete", "expired", expired)
	}
}

func (csts *ChunkStagingTrackingService) AddStagedChunk(chunk *stagedchunk.StagedChunk) {
	csts.mux.Lock()
	defer csts.mux.Unlock()
	csts.stagedChunks[chunk.OpId] = chunk
}

func (csts *ChunkStagingTrackingService) GetStagedChunk(opId string) *stagedchunk.StagedChunk {
	csts.mux.Lock()
	defer csts.mux.Unlock()
	return csts.stagedChunks[opId]
}

// AbortAll aborts all staged chunks and cleans up staging data.
// Called during graceful shutdown.
func (csts *ChunkStagingTrackingService) AbortAll() {
	csts.mux.Lock()
	defer csts.mux.Unlock()

	aborted := 0

	// Close all staged chunks
	for opId, sc := range csts.stagedChunks {
		sc.Close()
		delete(csts.stagedChunks, opId)
		aborted++
	}

	// Close all pending commits
	for chunkHandle, pending := range csts.pendingCommits {
		for seq, sc := range pending {
			sc.Close()
			delete(pending, seq)
			aborted++
		}
		delete(csts.pendingCommits, chunkHandle)
	}

	// Clear sequence tracking
	for k := range csts.nextExpectedSeq {
		delete(csts.nextExpectedSeq, k)
	}

	slog.Info("aborted all staged chunks", "count", aborted)
}

// CommitInOrder commits the staged chunk if its sequence is the next expected.
// If out of order, it buffers the commit for later.
// Returns list of chunks that were committed (in order).
func (csts *ChunkStagingTrackingService) CommitInOrder(sc *stagedchunk.StagedChunk) ([]*stagedchunk.StagedChunk, error) {
	csts.mux.Lock()
	defer csts.mux.Unlock()

	chunkHandle := sc.ChunkHandle
	seq := sc.Sequence

	// Get next expected sequence for this chunk (0 if first write)
	nextSeq := csts.nextExpectedSeq[chunkHandle]

	slog.Info("commit ordering check", "chunkHandle", chunkHandle, "writeSeq", seq, "nextExpected", nextSeq)

	if seq < nextSeq {
		// Already committed (duplicate or stale)
		slog.Warn("ignoring already committed sequence", "chunkHandle", chunkHandle, "seq", seq, "nextExpected", nextSeq)
		return nil, nil
	}

	if seq > nextSeq {
		// Out of order - buffer it
		slog.Info("buffering out-of-order commit", "chunkHandle", chunkHandle, "seq", seq, "nextExpected", nextSeq)
		if csts.pendingCommits[chunkHandle] == nil {
			csts.pendingCommits[chunkHandle] = make(map[uint64]*stagedchunk.StagedChunk)
		}
		csts.pendingCommits[chunkHandle][seq] = sc
		return nil, nil
	}

	// seq == nextSeq - commit this and any buffered successors
	var committed []*stagedchunk.StagedChunk

	// Commit this one
	if err := sc.Commit(); err != nil {
		return nil, err
	}
	committed = append(committed, sc)
	csts.nextExpectedSeq[chunkHandle] = seq + 1
	slog.Info("committed in order", "chunkHandle", chunkHandle, "seq", seq)

	// Check for buffered commits that can now proceed
	for {
		nextSeq = csts.nextExpectedSeq[chunkHandle]
		pending, ok := csts.pendingCommits[chunkHandle][nextSeq]
		if !ok {
			break
		}

		// Commit the buffered one
		if err := pending.Commit(); err != nil {
			slog.Error("failed to commit buffered write", "chunkHandle", chunkHandle, "seq", nextSeq, "error", err)
			return committed, err
		}
		committed = append(committed, pending)
		delete(csts.pendingCommits[chunkHandle], nextSeq)
		csts.nextExpectedSeq[chunkHandle] = nextSeq + 1
		slog.Info("committed buffered write", "chunkHandle", chunkHandle, "seq", nextSeq)
	}

	return committed, nil
}
