package chunkstagingtrackingservice

import (
	"sync"

	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
)

type ChunkStagingTrackingService struct {
	stagedChunks map[string]*stagedchunk.StagedChunk
	mux          sync.RWMutex
}

var (
	instance *ChunkStagingTrackingService
	once     sync.Once
)

// GetChunkStagingTrackingService returns the singleton instance
func GetChunkStagingTrackingService() *ChunkStagingTrackingService {
	once.Do(func() {
		instance = &ChunkStagingTrackingService{
			stagedChunks: make(map[string]*stagedchunk.StagedChunk),
		}
	})
	return instance
}

func (csts *ChunkStagingTrackingService) AddStagedChunk(chunk *stagedchunk.StagedChunk) {
	csts.mux.Lock()
	defer csts.mux.Unlock()
	csts.stagedChunks[chunk.OpId] = chunk
}

func (csts *ChunkStagingTrackingService) GetStagedChunk(opId string) *stagedchunk.StagedChunk {
	csts.mux.RLock()
	defer csts.mux.RUnlock()
	return csts.stagedChunks[opId]
}
