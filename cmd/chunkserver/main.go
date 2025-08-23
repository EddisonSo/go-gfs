package main

import (
	"fmt"

	"eddisonso.com/go-gfs/internal/chunkserver"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
)

func main() {
	fmt.Println("Starting Chunk Server...")
	config := csstructs.ChunkServerConfig{
		Hostname: "localhost",
		Port:     8080,
		Id:       "chunkserver-1",
		Dir:      "/tmp/chunkserver",
	}

	chunkserver := chunkserver.NewChunkServer(config)
	chunkserver.Start()
}
