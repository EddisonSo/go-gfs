package chunkserver

import (
	"eddisonso.com/go-gfs/internal/chunkserver/downloader"
	"eddisonso.com/go-gfs/internal/chunkserver/csconfig"
)

type ChunkServer struct {
	config csconfig.ChunkServerConfig
}


func NewChunkServer(config csconfig.ChunkServerConfig) *ChunkServer {
	return &ChunkServer{
		config: config,
	}
}

func (cs *ChunkServer) Start() {
	downloaderService := downloader.NewFileDownloadService(cs.config, 60)
	downloaderService.ListenAndServe()
}
