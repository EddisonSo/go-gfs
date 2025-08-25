package main

import (
	"fmt"
	"flag"
	"eddisonso.com/go-gfs/internal/chunkserver"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
)

func main() {
	fmt.Println("Starting Chunk Server...")
	
	dataPort := flag.Int("p", 8080, "Port for the chunk server to listen on")
	replicationPort := flag.Int("r", 8081, "Port for the chunk server replication service")
	hostname := flag.String("h", "localhost", "Hostname for the chunk server")

	flag.Parse()

	config := csstructs.ChunkServerConfig{
		Hostname: *hostname, 
		DataPort:     *dataPort,
		ReplicationPort: *replicationPort,
		Id:       "chunkserver-1",
		Dir:      "tmp/",
	}

	chunkserver := chunkserver.NewChunkServer(config)
	chunkserver.Start()
}
