package master

import (
	"context"
	"log/slog"

	pb "eddisonso.com/go-gfs/gen/master"
)

// GRPCServer implements the Master gRPC service
type GRPCServer struct {
	pb.UnimplementedMasterServer
	master *Master
}

// NewGRPCServer creates a new gRPC server for the master
func NewGRPCServer(m *Master) *GRPCServer {
	return &GRPCServer{master: m}
}

// Register handles chunkserver registration
func (s *GRPCServer) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	slog.Info("chunkserver registering",
		"serverID", req.ServerId,
		"hostname", req.Hostname,
		"dataPort", req.DataPort,
		"chunks", len(req.ChunkHandles))

	s.master.RegisterChunkServer(
		ChunkServerID(req.ServerId),
		req.Hostname,
		int(req.DataPort),
		int(req.ReplicationPort),
	)

	// Process chunk reports from registration
	for _, handle := range req.ChunkHandles {
		s.master.ReportChunk(ChunkServerID(req.ServerId), ChunkHandle(handle))
	}

	return &pb.RegisterResponse{
		Success: true,
		Message: "registered successfully",
	}, nil
}

// Heartbeat handles chunkserver heartbeats
func (s *GRPCServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	ok := s.master.Heartbeat(ChunkServerID(req.ServerId))
	if !ok {
		slog.Warn("heartbeat from unknown server", "serverID", req.ServerId)
		return &pb.HeartbeatResponse{
			Success: false,
		}, nil
	}

	// Process chunk reports
	for _, handle := range req.ChunkHandles {
		s.master.ReportChunk(ChunkServerID(req.ServerId), ChunkHandle(handle))
	}

	// Get chunks to delete
	pendingDeletes := s.master.GetPendingDeletes(ChunkServerID(req.ServerId))
	chunksToDelete := make([]string, len(pendingDeletes))
	for i, h := range pendingDeletes {
		chunksToDelete[i] = string(h)
	}

	if len(chunksToDelete) > 0 {
		slog.Info("sending chunks to delete", "serverID", req.ServerId, "chunks", chunksToDelete)
	}

	return &pb.HeartbeatResponse{
		Success:        true,
		ChunksToDelete: chunksToDelete,
	}, nil
}

// CreateFile creates a new file in the namespace
func (s *GRPCServer) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	file, err := s.master.CreateFile(req.Path)
	if err != nil {
		return &pb.CreateFileResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.CreateFileResponse{
		Success: true,
		Message: "file created",
		File:    fileInfoToProto(file),
	}, nil
}

// GetFile returns file metadata
func (s *GRPCServer) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	file, err := s.master.GetFile(req.Path)
	if err != nil {
		return &pb.GetFileResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.GetFileResponse{
		Success: true,
		File:    fileInfoToProto(file),
	}, nil
}

// DeleteFile removes a file from the namespace
func (s *GRPCServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	err := s.master.DeleteFile(req.Path)
	if err != nil {
		return &pb.DeleteFileResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.DeleteFileResponse{
		Success: true,
		Message: "file deleted",
	}, nil
}

// ListFiles returns all files in the namespace
func (s *GRPCServer) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	files := s.master.ListFiles()

	protoFiles := make([]*pb.FileInfoResponse, 0, len(files))
	for _, f := range files {
		// Filter by prefix if specified
		if req.Prefix != "" && len(f.Path) >= len(req.Prefix) {
			if f.Path[:len(req.Prefix)] != req.Prefix {
				continue
			}
		}
		protoFiles = append(protoFiles, fileInfoToProto(f))
	}

	return &pb.ListFilesResponse{
		Files: protoFiles,
	}, nil
}

// AllocateChunk allocates a new chunk for a file
func (s *GRPCServer) AllocateChunk(ctx context.Context, req *pb.AllocateChunkRequest) (*pb.AllocateChunkResponse, error) {
	chunkInfo, err := s.master.AddChunkToFile(req.Path)
	if err != nil {
		return &pb.AllocateChunkResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.AllocateChunkResponse{
		Success: true,
		Message: "chunk allocated",
		Chunk:   chunkInfoToProto(chunkInfo),
	}, nil
}

// GetChunkLocations returns chunk locations for a file
func (s *GRPCServer) GetChunkLocations(ctx context.Context, req *pb.GetChunkLocationsRequest) (*pb.GetChunkLocationsResponse, error) {
	chunks, err := s.master.GetFileChunks(req.Path)
	if err != nil {
		return &pb.GetChunkLocationsResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	protoChunks := make([]*pb.ChunkLocationInfo, 0, len(chunks))
	for _, c := range chunks {
		protoChunks = append(protoChunks, chunkInfoToProto(c))
	}

	return &pb.GetChunkLocationsResponse{
		Success: true,
		Chunks:  protoChunks,
	}, nil
}

// ReportCommit is called by chunkservers after successful 2PC commit
func (s *GRPCServer) ReportCommit(ctx context.Context, req *pb.ReportCommitRequest) (*pb.ReportCommitResponse, error) {
	slog.Info("received commit report",
		"serverID", req.ServerId,
		"chunkHandle", req.ChunkHandle,
		"size", req.Size)

	err := s.master.ConfirmChunkCommit(
		ChunkServerID(req.ServerId),
		ChunkHandle(req.ChunkHandle),
		req.Size,
	)
	if err != nil {
		return &pb.ReportCommitResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.ReportCommitResponse{
		Success: true,
		Message: "commit confirmed",
	}, nil
}

// Helper functions to convert internal types to protobuf types

func fileInfoToProto(f *FileInfo) *pb.FileInfoResponse {
	handles := make([]string, len(f.Chunks))
	for i, h := range f.Chunks {
		handles[i] = string(h)
	}

	return &pb.FileInfoResponse{
		Path:         f.Path,
		ChunkHandles: handles,
		Size:         f.Size,
		ChunkSize:    f.ChunkSize,
	}
}

func chunkInfoToProto(c *ChunkInfo) *pb.ChunkLocationInfo {
	locations := make([]*pb.ChunkServerInfo, len(c.Locations))
	for i, loc := range c.Locations {
		locations[i] = chunkLocationToProto(&loc)
	}

	var primary *pb.ChunkServerInfo
	if c.Primary != nil {
		primary = chunkLocationToProto(c.Primary)
	}

	return &pb.ChunkLocationInfo{
		ChunkHandle: string(c.Handle),
		Locations:   locations,
		Primary:     primary,
		Version:     c.Version,
		Size:        c.Size,
	}
}

func chunkLocationToProto(loc *ChunkLocation) *pb.ChunkServerInfo {
	return &pb.ChunkServerInfo{
		ServerId:        string(loc.ServerID),
		Hostname:        loc.Hostname,
		DataPort:        int32(loc.DataPort),
		ReplicationPort: int32(loc.ReplicationPort),
	}
}
