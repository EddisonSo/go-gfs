package main

import (
	"fmt"
	"flag"
	"eddisonso.com/go-gfs/internal/chunkserver"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
)

func main() {
	fmt.Println("Starting Chunk Server...")
	
	port := flag.Int("p", 8080, "Port for the chunk server to listen on")
	hostname := flag.String("h", "localhost", "Hostname for the chunk server")

	flag.Parse()

	config := csstructs.ChunkServerConfig{
		Hostname: *hostname, 
		Port:     *port,
		Id:       "chunkserver-1",
		Dir:      "tmp/",
	}

	chunkserver := chunkserver.NewChunkServer(config)
	chunkserver.Start()
}
