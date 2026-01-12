package gfs

import (
	"context"
	"io"
	"os"
)

// Write appends data to the file at path.
func (c *Client) Write(ctx context.Context, path string, data []byte) (int, error) {
	return c.Append(ctx, path, data)
}

// WriteFromFile appends the contents of a local file to the file at path.
func (c *Client) WriteFromFile(ctx context.Context, path, localPath string) (int64, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	return c.AppendFrom(ctx, path, file)
}

// WriteAt overwrites data starting at the provided offset.
func (c *Client) WriteAt(ctx context.Context, path string, data []byte, offset int64) (int, error) {
	return c.writeAtData(ctx, path, data, offset)
}

// WriteFromFileAt overwrites data from a local file starting at the provided offset.
func (c *Client) WriteFromFileAt(ctx context.Context, path, localPath string, offset int64) (int64, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	buf := make([]byte, int(c.maxChunkSize))
	var total int64
	currentOffset := offset

	for {
		n, err := file.Read(buf)
		if n > 0 {
			written, writeErr := c.writeAtData(ctx, path, buf[:n], currentOffset)
			total += int64(written)
			currentOffset += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// Read reads the entire file into memory.
func (c *Client) Read(ctx context.Context, path string) ([]byte, error) {
	return c.ReadFile(ctx, path)
}

// ReadToFile writes file contents to a local file.
func (c *Client) ReadToFile(ctx context.Context, path, localPath string) (int64, error) {
	out, err := os.Create(localPath)
	if err != nil {
		return 0, err
	}
	defer out.Close()

	return c.ReadTo(ctx, path, out)
}
