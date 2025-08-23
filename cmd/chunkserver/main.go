package main

import (
	"fmt"

	"eddisonso.com/go-gfs/internal/chunkserver"
	"eddisonso.com/go-gfs/internal/chunkserver/csconfig"
)

func main() {
	fmt.Println("Starting Chunk Server...")
	config := csconfig.ChunkServerConfig{
		Hostname: "localhost",
		Port:     8080,
		Id:       "chunkserver-1",
		Dir:      "/tmp/chunkserver",
	}

	chunkserver := chunkserver.NewChunkServer(config)
	chunkserver.Start()
}
