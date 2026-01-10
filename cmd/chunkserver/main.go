package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"eddisonso.com/go-gfs/internal/chunkserver"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/masterclient"
)

func main() {
	slog.Info("starting chunkserver")

	dataPort := flag.Int("p", 8080, "Port for the chunk server to listen on")
	replicationPort := flag.Int("r", 8081, "Port for the chunk server replication service")
	hostname := flag.String("h", "localhost", "Hostname for the chunk server")
	dir := flag.String("d", "tmp/", "Directory for chunk storage")
	id := flag.String("id", "chunkserver-1", "Chunk server ID")
	masterAddr := flag.String("master", "", "Master server address (e.g., localhost:9000). If empty, runs standalone.")
	heartbeatInterval := flag.Duration("heartbeat", 10*time.Second, "Heartbeat interval to master")

	flag.Parse()

	config := csstructs.ChunkServerConfig{
		Hostname:        *hostname,
		DataPort:        *dataPort,
		ReplicationPort: *replicationPort,
		Id:              *id,
		Dir:             *dir,
	}

	// Create and start chunkserver
	cs := chunkserver.NewChunkServer(config)
	go cs.Start()

	// Connect to master if address provided
	var mc *masterclient.MasterClient
	if *masterAddr != "" {
		mc = masterclient.NewMasterClient(
			*id,
			*hostname,
			*dataPort,
			*replicationPort,
			*dir,
			*masterAddr,
		)

		if err := mc.Connect(); err != nil {
			slog.Error("failed to connect to master", "addr", *masterAddr, "error", err)
			os.Exit(1)
		}

		if err := mc.Register(); err != nil {
			slog.Error("failed to register with master", "error", err)
			os.Exit(1)
		}

		// Set singleton instance so downloader can report commits
		masterclient.SetInstance(mc)

		mc.StartHeartbeat(*heartbeatInterval)
	} else {
		slog.Warn("running without master (standalone mode)")
	}

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("shutting down chunkserver")
	if mc != nil {
		mc.Close()
	}
}
