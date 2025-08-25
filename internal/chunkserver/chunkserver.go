package chunkserver

import (
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/dataplane"
	"eddisonso.com/go-gfs/internal/chunkserver/replicationplane"
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
	dataPlane := dataplane.NewDataPlane(cs.config)
	replicationPlane := replicationplane.NewReplicationPlane(cs.config)
	go dataPlane.Start()
	replicationPlane.Start()
}
