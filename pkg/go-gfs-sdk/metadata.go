package gfs

import (
	"context"
	"fmt"

	pb "eddisonso.com/go-gfs/gen/master"
)

// CreateFile creates a new file entry.
func (c *Client) CreateFile(ctx context.Context, path string) (*pb.FileInfoResponse, error) {
	resp, err := c.master.CreateFile(ctx, &pb.CreateFileRequest{Path: path})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("create file failed: %s", resp.Message)
	}
	return resp.File, nil
}

// GetFile returns file metadata.
func (c *Client) GetFile(ctx context.Context, path string) (*pb.FileInfoResponse, error) {
	resp, err := c.master.GetFile(ctx, &pb.GetFileRequest{Path: path})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("get file failed: %s", resp.Message)
	}
	return resp.File, nil
}

// DeleteFile removes a file entry.
func (c *Client) DeleteFile(ctx context.Context, path string) error {
	resp, err := c.master.DeleteFile(ctx, &pb.DeleteFileRequest{Path: path})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("delete file failed: %s", resp.Message)
	}
	return nil
}

// RenameFile renames a file.
func (c *Client) RenameFile(ctx context.Context, oldPath, newPath string) error {
	resp, err := c.master.RenameFile(ctx, &pb.RenameFileRequest{OldPath: oldPath, NewPath: newPath})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("rename file failed: %s", resp.Message)
	}
	return nil
}

// ListFiles lists files under a prefix.
func (c *Client) ListFiles(ctx context.Context, prefix string) ([]*pb.FileInfoResponse, error) {
	resp, err := c.master.ListFiles(ctx, &pb.ListFilesRequest{Prefix: prefix})
	if err != nil {
		return nil, err
	}
	return resp.Files, nil
}

// AllocateChunk requests a new chunk for a file.
func (c *Client) AllocateChunk(ctx context.Context, path string) (*pb.ChunkLocationInfo, error) {
	resp, err := c.master.AllocateChunk(ctx, &pb.AllocateChunkRequest{Path: path})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("allocate chunk failed: %s", resp.Message)
	}
	return resp.Chunk, nil
}

// GetChunkLocations returns chunk locations for a file.
func (c *Client) GetChunkLocations(ctx context.Context, path string) ([]*pb.ChunkLocationInfo, error) {
	resp, err := c.master.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: path})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("get chunk locations failed: %s", resp.Message)
	}
	return resp.Chunks, nil
}

// GetClusterPressure retrieves cluster resource information.
func (c *Client) GetClusterPressure(ctx context.Context) (*pb.GetClusterPressureResponse, error) {
	return c.master.GetClusterPressure(ctx, &pb.GetClusterPressureRequest{})
}

// ProgressFunc is called after each chunk write with bytes written so far.
type ProgressFunc func(bytesWritten int64)

// PreparedUpload holds pre-allocated chunks for efficient uploads.
type PreparedUpload struct {
	client       *Client
	path         string
	chunks       []*pb.ChunkLocationInfo
	index        int // current chunk index
	bytesWritten int64
	onProgress   ProgressFunc
}

// OnProgress sets a callback that's invoked after each chunk write completes.
// The callback receives the total bytes written so far.
func (p *PreparedUpload) OnProgress(fn ProgressFunc) {
	p.onProgress = fn
}

// PrepareUpload pre-allocates chunks for a file of the given size.
// This eliminates allocation delays during streaming uploads.
func (c *Client) PrepareUpload(ctx context.Context, path string, size int64) (*PreparedUpload, error) {
	// Create file if it doesn't exist
	c.CreateFile(ctx, path)

	// Get existing chunks
	existing, _ := c.GetChunkLocations(ctx, path)

	// Calculate how many chunks we need
	chunksNeeded := int((size + c.maxChunkSize - 1) / c.maxChunkSize)
	if chunksNeeded == 0 {
		chunksNeeded = 1
	}

	// Calculate space available in last existing chunk
	var spaceInLast int64
	if len(existing) > 0 {
		lastChunk := existing[len(existing)-1]
		spaceInLast = c.maxChunkSize - int64(lastChunk.Size)
		if spaceInLast > 0 {
			// Account for space in last chunk
			remainingAfterLast := size - spaceInLast
			if remainingAfterLast <= 0 {
				chunksNeeded = 0 // existing chunk has enough space
			} else {
				chunksNeeded = int((remainingAfterLast + c.maxChunkSize - 1) / c.maxChunkSize)
			}
		}
	}

	// Pre-allocate all needed chunks
	chunks := make([]*pb.ChunkLocationInfo, 0, len(existing)+chunksNeeded)
	chunks = append(chunks, existing...)

	for i := 0; i < chunksNeeded; i++ {
		chunk, err := c.AllocateChunk(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("failed to pre-allocate chunk %d: %w", i, err)
		}
		chunks = append(chunks, chunk)
	}

	return &PreparedUpload{
		client: c,
		path:   path,
		chunks: chunks,
		index:  0,
	}, nil
}

// ChunkCount returns the number of pre-allocated chunks.
func (p *PreparedUpload) ChunkCount() int {
	return len(p.chunks)
}

// Path returns the file path.
func (p *PreparedUpload) Path() string {
	return p.path
}
