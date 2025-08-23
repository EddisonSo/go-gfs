package chunkserver

import (
	"eddisonso.com/go-gfs/internal/chunkserver/downloader"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
)

type ChunkServer struct {
	config csstructs.ChunkServerConfig
}


func NewChunkServer(config csstructs.ChunkServerConfig) *ChunkServer {
	return &ChunkServer{
		config: config,
	}
}

func (cs *ChunkServer) Start() {
	downloaderService := downloader.NewFileDownloadService(cs.config, 60)
	downloaderService.ListenAndServe()
}
