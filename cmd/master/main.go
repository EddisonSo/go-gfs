package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	pb "eddisonso.com/go-gfs/gen/master"
	"eddisonso.com/go-gfs/internal/master"
	"eddisonso.com/go-gfs/pkg/gfslog"
	"google.golang.org/grpc"
)

var (
	port           int
	dataDir        string
	logServiceAddr string
	logSource      string
)

func init() {
	flag.IntVar(&port, "port", 9000, "Master server port")
	flag.StringVar(&dataDir, "data", "/data/master", "Data directory for WAL")
	flag.StringVar(&logServiceAddr, "log-service", "", "Log service address (e.g., log-service:50051)")
	flag.StringVar(&logSource, "log-source", "gfs-master", "Log source name (e.g., pod name)")
}

func main() {
	flag.Parse()

	// Initialize logger
	if logServiceAddr != "" {
		logger := gfslog.NewLogger(gfslog.Config{
			Source:         logSource,
			LogServiceAddr: logServiceAddr,
			MinLevel:       slog.LevelDebug,
		})
		slog.SetDefault(logger.Logger)
		defer logger.Close()
	}

	slog.Info("starting master server", "port", port, "dataDir", dataDir)

	// Create master instance with WAL
	walPath := filepath.Join(dataDir, "wal.log")
	m, err := master.NewMaster(walPath)
	if err != nil {
		slog.Error("failed to create master", "error", err)
		os.Exit(1)
	}
	defer m.Close()

	// Create gRPC server
	grpcServer := grpc.NewServer()
	masterService := master.NewGRPCServer(m)
	pb.RegisterMasterServer(grpcServer, masterService)

	// Start listening
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
	}

	// Handle graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		slog.Info("shutting down master server")
		grpcServer.GracefulStop()
	}()

	slog.Info("master server listening", "port", port)
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("failed to serve", "error", err)
		os.Exit(1)
	}
}
