package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	pb "eddisonso.com/go-gfs/gen/filetransfer"
	"eddisonso.com/go-gfs/internal/chunkserver"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterFileServiceServer(grpcServer, &chunkserver.FileServiceServer{})

	log.Println("gRPC server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
