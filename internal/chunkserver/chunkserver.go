package chunkserver

import (
	"context"
	pb "eddisonso.com/go-gfs/gen/filetransfer"
)

type ChunkServiceServer struct {
	pb.UnimplementedChunkServiceServer
}

func (s *ChunkServiceServer) UploadChunk(ctx context.Context, req *pb.UploadChunkRequest) (*pb.UploadChunkResponse, error) {
	// TODO: Implement upload logic
	chunkId := req.GetChunkId()
	nBytes := req.GetBytes()

	

	return &pb.UploadChunkResponse{
		Success:       true,
		Transactionid: "TRANSACTION_ID",
	}, nil
}

func (s *ChunkServiceServer) DownloadChunk(ctx context.Context, req *pb.DownloadChunkRequest) (*pb.DownloadChunkResponse, error) {
	// TODO: Implement download logic
	chunkId := req.GetChunkId()

	

	return &pb.DownloadChunkResponse{
		Success:       true,
		Transactionid: "TRANSACTION_ID",
	}, nil
}

