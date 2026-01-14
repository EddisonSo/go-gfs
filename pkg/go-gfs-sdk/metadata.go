package gfs

import (
	"context"
	"fmt"

	pb "eddisonso.com/go-gfs/gen/master"
)

// DefaultNamespace is used when no namespace is provided.
const DefaultNamespace = "default"

func normalizeNamespace(namespace string) string {
	if namespace == "" {
		return DefaultNamespace
	}
	return namespace
}

// CreateFile creates a new file entry.
func (c *Client) CreateFile(ctx context.Context, path string) (*pb.FileInfoResponse, error) {
	return c.CreateFileWithNamespace(ctx, path, "")
}

// CreateFileWithNamespace creates a new file entry with a namespace.
func (c *Client) CreateFileWithNamespace(ctx context.Context, path, namespace string) (*pb.FileInfoResponse, error) {
	resp, err := c.master.CreateFile(ctx, &pb.CreateFileRequest{
		Path:      path,
		Namespace: normalizeNamespace(namespace),
	})
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
	return c.GetFileWithNamespace(ctx, path, "")
}

// GetFileWithNamespace returns file metadata with a namespace.
func (c *Client) GetFileWithNamespace(ctx context.Context, path, namespace string) (*pb.FileInfoResponse, error) {
	resp, err := c.master.GetFile(ctx, &pb.GetFileRequest{
		Path:      path,
		Namespace: normalizeNamespace(namespace),
	})
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
	return c.DeleteFileWithNamespace(ctx, path, "")
}

// DeleteFileWithNamespace removes a file entry in a namespace.
func (c *Client) DeleteFileWithNamespace(ctx context.Context, path, namespace string) error {
	ns := normalizeNamespace(namespace)
	resp, err := c.master.DeleteFile(ctx, &pb.DeleteFileRequest{
		Path:      path,
		Namespace: ns,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("delete file failed: %s", resp.Message)
	}
	c.invalidateChunkCache(path, ns)
	c.forgetFile(path, ns)
	return nil
}

// DeleteNamespace removes a namespace and all its files.
func (c *Client) DeleteNamespace(ctx context.Context, namespace string) (int, error) {
	resp, err := c.master.DeleteNamespace(ctx, &pb.DeleteNamespaceRequest{
		Namespace: namespace,
	})
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, fmt.Errorf("delete namespace failed: %s", resp.Message)
	}
	c.invalidateNamespaceCache(namespace)
	c.forgetNamespace(namespace)
	return int(resp.FilesDeleted), nil
}

// RenameFile renames a file.
func (c *Client) RenameFile(ctx context.Context, oldPath, newPath string) error {
	return c.RenameFileWithNamespace(ctx, oldPath, newPath, "")
}

// RenameFileWithNamespace renames a file within a namespace.
func (c *Client) RenameFileWithNamespace(ctx context.Context, oldPath, newPath, namespace string) error {
	resp, err := c.master.RenameFile(ctx, &pb.RenameFileRequest{
		OldPath:   oldPath,
		NewPath:   newPath,
		Namespace: normalizeNamespace(namespace),
	})
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
	return c.ListFilesWithNamespace(ctx, "", prefix)
}

// ListFilesWithNamespace lists files under a prefix, optionally scoped to a namespace.
func (c *Client) ListFilesWithNamespace(ctx context.Context, namespace, prefix string) ([]*pb.FileInfoResponse, error) {
	resp, err := c.master.ListFiles(ctx, &pb.ListFilesRequest{
		Prefix:    prefix,
		Namespace: namespace,
	})
	if err != nil {
		return nil, err
	}
	return resp.Files, nil
}

// AllocateChunk requests a new chunk for a file.
func (c *Client) AllocateChunk(ctx context.Context, path string) (*pb.ChunkLocationInfo, error) {
	return c.AllocateChunkWithNamespace(ctx, path, "")
}

// AllocateChunkWithNamespace requests a new chunk for a file in a namespace.
func (c *Client) AllocateChunkWithNamespace(ctx context.Context, path, namespace string) (*pb.ChunkLocationInfo, error) {
	resp, err := c.master.AllocateChunk(ctx, &pb.AllocateChunkRequest{
		Path:      path,
		Namespace: normalizeNamespace(namespace),
	})
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
	return c.GetChunkLocationsWithNamespace(ctx, path, "")
}

// GetChunkLocationsWithNamespace returns chunk locations for a file in a namespace.
func (c *Client) GetChunkLocationsWithNamespace(ctx context.Context, path, namespace string) ([]*pb.ChunkLocationInfo, error) {
	resp, err := c.master.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{
		Path:      path,
		Namespace: normalizeNamespace(namespace),
	})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("get chunk locations failed: %s", resp.Message)
	}
	return resp.Chunks, nil
}

// ProgressFunc is called after each chunk write with bytes written so far.
type ProgressFunc func(bytesWritten int64)

// PreparedUpload holds state for streaming uploads with on-demand chunk allocation.
type PreparedUpload struct {
	client       *Client
	path         string
	namespace    string
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

// PrepareUpload prepares for a streaming upload. Chunks are allocated on-demand.
func (c *Client) PrepareUpload(ctx context.Context, path string, size int64) (*PreparedUpload, error) {
	return c.PrepareUploadWithNamespace(ctx, path, "", size)
}

// PrepareUploadWithNamespace prepares for a streaming upload with a namespace.
// Chunks are allocated on-demand as data is written, not pre-allocated.
func (c *Client) PrepareUploadWithNamespace(ctx context.Context, path, namespace string, size int64) (*PreparedUpload, error) {
	normalizedNamespace := normalizeNamespace(namespace)
	// Create file if it doesn't exist
	c.CreateFileWithNamespace(ctx, path, normalizedNamespace)

	// Get existing chunks (we'll allocate more on-demand as needed)
	existing, _ := c.GetChunkLocationsWithNamespace(ctx, path, normalizedNamespace)

	return &PreparedUpload{
		client:    c,
		path:      path,
		namespace: normalizedNamespace,
		chunks:    existing,
		index:     0,
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
