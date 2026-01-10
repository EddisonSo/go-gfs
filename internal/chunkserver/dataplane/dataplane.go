package dataplane

import (
	"net"
	"strconv"
	"log/slog"
	"encoding/binary"
	"eddisonso.com/go-gfs/internal/chunkserver/downloader"
	"eddisonso.com/go-gfs/internal/chunkserver/uploader"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
)

type DataPlane struct {
	config csstructs.ChunkServerConfig
}

func NewDataPlane(config csstructs.ChunkServerConfig) *DataPlane {
	return &DataPlane {
		config: config,
	}
}

func (cs *DataPlane) Start() {
	ln, err := net.Listen("tcp", cs.config.Hostname + ":" + strconv.Itoa(cs.config.DataPort))
	if err != nil {
		slog.Error("Failed to start listener", "address", cs.config.Hostname + ":" + strconv.Itoa(cs.config.DataPort), "error", err)
	}

	downloader, err:= downloader.NewFileDownloadService(cs.config, 30)
	if err != nil {
		slog.Error("Failed to create FileDownloadService", "error", err)
		return
	}

	uploader := uploader.NewFileUploadService(cs.config)

	slog.Info("DataPlane is listening", "address", cs.config.Hostname + ":" + strconv.Itoa(cs.config.DataPort))
	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("Failed to accept connection", "error", err)
			continue
		}
		slog.Info("New connection established", "remote_addr", conn.RemoteAddr().String())

		buf := make([]byte, 4)
		n, err := conn.Read(buf)
		if err != nil || n != 4 {
			slog.Error("Failed to read action from connection", "error", err)
			conn.Close()
			continue
		}

		action := csstructs.Action(binary.BigEndian.Uint32(buf))
		switch action {
		case csstructs.Download:
			go downloader.HandleDownload(conn)
		case csstructs.Upload:
			go uploader.HandleUpload(conn)
		}
	}
}
