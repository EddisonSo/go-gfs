package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	pb "eddisonso.com/go-gfs/gen/master"
	"eddisonso.com/go-gfs/internal/master"
	"google.golang.org/grpc"
)

var (
	port int
)

func init() {
	flag.IntVar(&port, "port", 9000, "Master server port")
}

func main() {
	flag.Parse()

	slog.Info("starting master server", "port", port)

	// Create master instance
	m := master.NewMaster()

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
