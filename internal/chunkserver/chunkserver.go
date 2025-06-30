package chunkserver

import (
	"context"
	pb "eddisonso.com/go-gfs/gen/filetransfer"
)

type FileServiceServer struct {
	pb.UnimplementedFileServiceServer
}

func (s *FileServiceServer) UploadFile(ctx context.Context, req *pb.UploadFileRequest) (*pb.UploadFileResponse, error) {
	// TODO: Implement upload logic
	return &pb.UploadFileResponse{
		Success:       true,
		Transactionid: "TRANSACTION_ID",
	}, nil
}

func (s *FileServiceServer) DownloadFile(ctx context.Context, req *pb.DownloadFileRequest) (*pb.DownloadFileResponse, error) {
	// TODO: Implement download logic
	return &pb.DownloadFileResponse{
		Success:       true,
		Transactionid: "TRANSACTION_ID",
	}, nil
}

