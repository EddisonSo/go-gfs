package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// OpType represents the type of operation in the WAL
type OpType string

const (
	OpCreateFile   OpType = "CREATE_FILE"
	OpDeleteFile   OpType = "DELETE_FILE"
	OpRenameFile   OpType = "RENAME_FILE"
	OpAddChunk     OpType = "ADD_CHUNK"
	OpCommitChunk  OpType = "COMMIT_CHUNK"
	OpSetCounter   OpType = "SET_COUNTER"
)

// Entry represents a single WAL entry
type Entry struct {
	Op   OpType          `json:"op"`
	Data json.RawMessage `json:"data"`
}

// CreateFileData represents data for CREATE_FILE operation
type CreateFileData struct {
	Path      string `json:"path"`
	ChunkSize uint64 `json:"chunk_size"`
}

// DeleteFileData represents data for DELETE_FILE operation
type DeleteFileData struct {
	Path string `json:"path"`
}

// RenameFileData represents data for RENAME_FILE operation
type RenameFileData struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
}

// AddChunkData represents data for ADD_CHUNK operation
type AddChunkData struct {
	Path        string `json:"path"`
	ChunkHandle string `json:"chunk_handle"`
}

// CommitChunkData represents data for COMMIT_CHUNK operation
type CommitChunkData struct {
	ChunkHandle string `json:"chunk_handle"`
	Size        uint64 `json:"size"`
}

// SetCounterData represents data for SET_COUNTER operation
type SetCounterData struct {
	NextChunkHandle uint64 `json:"next_chunk_handle"`
}

// WAL is a write-ahead log for master persistence
type WAL struct {
	file *os.File
	mu   sync.Mutex
	path string
}

// New creates a new WAL at the given path
func New(path string) (*WAL, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file: file,
		path: path,
	}, nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// append writes an entry to the WAL
func (w *WAL) append(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	data = append(data, '\n')
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Sync to disk for durability
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	return nil
}

// LogCreateFile logs a CREATE_FILE operation
func (w *WAL) LogCreateFile(path string, chunkSize uint64) error {
	data, _ := json.Marshal(CreateFileData{Path: path, ChunkSize: chunkSize})
	return w.append(Entry{Op: OpCreateFile, Data: data})
}

// LogDeleteFile logs a DELETE_FILE operation
func (w *WAL) LogDeleteFile(path string) error {
	data, _ := json.Marshal(DeleteFileData{Path: path})
	return w.append(Entry{Op: OpDeleteFile, Data: data})
}

// LogRenameFile logs a RENAME_FILE operation
func (w *WAL) LogRenameFile(oldPath, newPath string) error {
	data, _ := json.Marshal(RenameFileData{OldPath: oldPath, NewPath: newPath})
	return w.append(Entry{Op: OpRenameFile, Data: data})
}

// LogAddChunk logs an ADD_CHUNK operation
func (w *WAL) LogAddChunk(path, chunkHandle string) error {
	data, _ := json.Marshal(AddChunkData{Path: path, ChunkHandle: chunkHandle})
	return w.append(Entry{Op: OpAddChunk, Data: data})
}

// LogCommitChunk logs a COMMIT_CHUNK operation
func (w *WAL) LogCommitChunk(chunkHandle string, size uint64) error {
	data, _ := json.Marshal(CommitChunkData{ChunkHandle: chunkHandle, Size: size})
	return w.append(Entry{Op: OpCommitChunk, Data: data})
}

// LogSetCounter logs the chunk handle counter
func (w *WAL) LogSetCounter(nextChunkHandle uint64) error {
	data, _ := json.Marshal(SetCounterData{NextChunkHandle: nextChunkHandle})
	return w.append(Entry{Op: OpSetCounter, Data: data})
}

// Reader provides an interface for replaying WAL entries
type Reader struct {
	scanner *bufio.Scanner
	file    *os.File
}

// NewReader creates a reader to replay WAL entries
func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No WAL file, nothing to replay
		}
		return nil, fmt.Errorf("failed to open WAL for reading: %w", err)
	}

	return &Reader{
		scanner: bufio.NewScanner(file),
		file:    file,
	}, nil
}

// Close closes the reader
func (r *Reader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// ReadAll reads all entries from the WAL
func (r *Reader) ReadAll() ([]Entry, error) {
	if r == nil {
		return nil, nil
	}

	var entries []Entry
	for r.scanner.Scan() {
		line := r.scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry Entry
		if err := json.Unmarshal(line, &entry); err != nil {
			slog.Warn("skipping malformed WAL entry", "error", err)
			continue
		}
		entries = append(entries, entry)
	}

	if err := r.scanner.Err(); err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading WAL: %w", err)
	}

	return entries, nil
}
