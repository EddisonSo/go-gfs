package chunkserver

import (
	"net"
	"strconv"
	"log/slog"
	"encoding/binary"
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
	ln, err := net.Listen("tcp", cs.config.Hostname + ":" + strconv.Itoa(cs.config.Port))
	if err != nil {
		slog.Error("Failed to start listener", "address", cs.config.Hostname + ":" + strconv.Itoa(cs.config.Port), "error", err)
	}

	downloader, err:= downloader.NewFileDownloadService(cs.config, 30)
	if err != nil {
		slog.Error("Failed to create FileDownloadService", "error", err)
		return
	}

	slog.Info("FileDownloadService is listening", "address", cs.config.Hostname + ":" + strconv.Itoa(cs.config.Port))
	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("Failed to accept connection", "error", err)
			continue
		}
		buf := make([]byte, 4)
		action := csstructs.Action(binary.BigEndian.Uint32(buf))
		switch action {
			case csstructs.Download:
				go downloader.HandleDownload(conn)
				
		}
	}
}
