package chunkstagingtrackingservice

import (
	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
)

type ChunkStagingTrackingService struct {
	stagedChunks map[string]*stagedchunk.StagedChunk
}

//use singleton pattern here
func GetChunkStagingTrackingService() *ChunkStagingTrackingService{
	return &ChunkStagingTrackingService{
		stagedChunks: make(map[string]*stagedchunk.StagedChunk),
	}
}

func (csts *ChunkStagingTrackingService) AddStagedChunk(chunk *stagedchunk.StagedChunk) {
	csts.stagedChunks[chunk.OpId] = chunk
}
