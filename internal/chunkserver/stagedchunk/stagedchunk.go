package stagedchunk

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
)

// Threshold for using temp file vs memory buffer (1MB)
const fileStagingThreshold = 1 << 20

type StagedChunk struct {
	ChunkHandle string
	OpId        string
	// Memory buffer for small writes
	buf []byte
	pos int
	// Temp file for large writes
	tempFile   *os.File
	useFile    bool
	written    int64
	size       uint64
	mux        sync.Mutex
	Offset     uint64
	Sequence   uint64
	Status     csstructs.Status
	ready      uint8
	storageDir string
	CreatedAt  time.Time
}

func NewStagedChunk(chunkHandle string, opId string, size uint64, offset uint64, sequence uint64, storageDir string) *StagedChunk {
	sc := &StagedChunk{
		ChunkHandle: chunkHandle,
		OpId:        opId,
		size:        size,
		Offset:      offset,
		Sequence:    sequence,
		Status:      csstructs.READY,
		ready:       0,
		storageDir:  storageDir,
		CreatedAt:   time.Now(),
	}

	// Use temp file for large writes, memory buffer for small ones
	if size >= fileStagingThreshold {
		if err := os.MkdirAll(storageDir, 0755); err != nil {
			slog.Error("failed to create storage directory", "dir", storageDir, "error", err)
			return nil
		}

		tempFile, err := os.CreateTemp(storageDir, "staged_"+opId+"_*")
		if err != nil {
			slog.Error("failed to create temp file for staging", "opId", opId, "error", err)
			return nil
		}

		sc.tempFile = tempFile
		sc.useFile = true
		slog.Debug("using temp file for large write", "path", tempFile.Name(), "opId", opId, "size", size)
	} else {
		sc.buf = make([]byte, size)
		sc.useFile = false
		slog.Debug("using memory buffer for small write", "opId", opId, "size", size)
	}

	return sc
}

func (sc *StagedChunk) Write(p []byte) (int, error) {
	sc.mux.Lock()
	defer sc.mux.Unlock()

	if sc.useFile {
		if sc.tempFile == nil {
			return 0, fmt.Errorf("temp file not initialized")
		}
		n, err := sc.tempFile.Write(p)
		if err != nil {
			return n, err
		}
		sc.written += int64(n)
		return n, nil
	}

	// Memory buffer path
	if sc.pos >= len(sc.buf) {
		return 0, io.EOF
	}
	n := copy(sc.buf[sc.pos:], p)
	sc.pos += n
	return n, nil
}

func (sc *StagedChunk) NewReader() io.Reader {
	sc.mux.Lock()
	defer sc.mux.Unlock()

	if sc.useFile {
		if sc.tempFile == nil {
			return nil
		}
		sc.tempFile.Seek(0, io.SeekStart)
		return sc.tempFile
	}

	return bytes.NewReader(sc.buf[:sc.pos])
}

func (sc *StagedChunk) Bytes() []byte {
	sc.mux.Lock()
	defer sc.mux.Unlock()

	if sc.useFile {
		if sc.tempFile == nil {
			return nil
		}
		sc.tempFile.Seek(0, io.SeekStart)
		data, err := io.ReadAll(sc.tempFile)
		if err != nil {
			slog.Error("failed to read temp file", "error", err)
			return nil
		}
		return data
	}

	cp := make([]byte, sc.pos)
	copy(cp, sc.buf[:sc.pos])
	return cp
}

func (sc *StagedChunk) Len() uint64 {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	if sc.useFile {
		return uint64(sc.written)
	}
	return uint64(sc.pos)
}

func (sc *StagedChunk) Cap() uint64 {
	return sc.size
}

func (sc *StagedChunk) Pos() uint64 {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	if sc.useFile {
		return uint64(sc.written)
	}
	return uint64(sc.pos)
}

func (sc *StagedChunk) Commit() error {
	sc.mux.Lock()
	defer sc.mux.Unlock()

	var dataSize int64
	if sc.useFile {
		dataSize = sc.written
	} else {
		dataSize = int64(sc.pos)
	}

	slog.Info("starting commit", "opID", sc.OpId, "chunkHandle", sc.ChunkHandle, "offset", sc.Offset, "size", dataSize, "useFile", sc.useFile)

	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(sc.storageDir, 0755); err != nil {
		slog.Error("failed to create storage directory", "dir", sc.storageDir, "error", err)
		return fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Chunk file path
	chunkFilePath := filepath.Join(sc.storageDir, sc.ChunkHandle)
	slog.Info("writing to file", "path", chunkFilePath)

	var bytesWritten int64

	// Optimization: rename temp file instead of copying for new chunks at offset 0
	if sc.useFile && sc.Offset == 0 {
		if sc.tempFile == nil {
			return fmt.Errorf("temp file not initialized")
		}

		tempPath := sc.tempFile.Name()

		// Check if chunk file exists
		_, statErr := os.Stat(chunkFilePath)
		chunkExists := statErr == nil

		if !chunkExists {
			// Fast path: rename temp file to chunk file
			sc.tempFile.Sync() // Ensure temp file is flushed
			sc.tempFile.Close()
			sc.tempFile = nil

			if err := os.Rename(tempPath, chunkFilePath); err != nil {
				slog.Error("failed to rename temp file", "from", tempPath, "to", chunkFilePath, "error", err)
				return fmt.Errorf("failed to rename temp file: %w", err)
			}

			bytesWritten = dataSize
			slog.Info("renamed temp file to chunk file (fast path)", "bytes", bytesWritten)

			sc.Status = csstructs.COMMIT
			slog.Info("COMMIT SUCCESSFUL - renamed temp file", "opID", sc.OpId, "chunkHandle", sc.ChunkHandle, "file", chunkFilePath, "bytesWritten", bytesWritten)
			return nil
		}
	}

	// Slow path: copy data to chunk file (for random writes or existing chunks)

	// Open or create the chunk file
	file, err := os.OpenFile(chunkFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("failed to open chunk file", "path", chunkFilePath, "error", err)
		return fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer file.Close()

	// Seek to the offset position
	if _, err := file.Seek(int64(sc.Offset), io.SeekStart); err != nil {
		slog.Error("failed to seek", "offset", sc.Offset, "error", err)
		return fmt.Errorf("failed to seek to offset %d: %w", sc.Offset, err)
	}

	if sc.useFile {
		// Stream from temp file to chunk file
		if sc.tempFile == nil {
			return fmt.Errorf("temp file not initialized")
		}

		tempPath := sc.tempFile.Name()

		// Seek temp file to beginning for reading
		if _, err := sc.tempFile.Seek(0, io.SeekStart); err != nil {
			slog.Error("failed to seek temp file", "error", err)
			return fmt.Errorf("failed to seek temp file: %w", err)
		}

		bytesWritten, err = io.Copy(file, sc.tempFile)
		if err != nil {
			slog.Error("failed to write data", "error", err)
			return fmt.Errorf("failed to write data to disk: %w", err)
		}

		// Close and remove temp file
		sc.tempFile.Close()
		if err := os.Remove(tempPath); err != nil {
			slog.Warn("failed to remove temp file", "path", tempPath, "error", err)
		}
		sc.tempFile = nil
	} else {
		// Write from memory buffer
		n, err := file.Write(sc.buf[:sc.pos])
		if err != nil {
			slog.Error("failed to write data", "error", err)
			return fmt.Errorf("failed to write data to disk: %w", err)
		}
		bytesWritten = int64(n)
		// Clear buffer to free memory
		sc.buf = nil
	}

	slog.Info("wrote data to disk", "bytes", bytesWritten)

	if bytesWritten != dataSize {
		slog.Error("incomplete write", "wrote", bytesWritten, "expected", dataSize)
		return fmt.Errorf("incomplete write: wrote %d bytes, expected %d", bytesWritten, dataSize)
	}

	// Sync to ensure durability
	if err := file.Sync(); err != nil {
		slog.Error("failed to sync", "error", err)
		return fmt.Errorf("failed to sync data to disk: %w", err)
	}
	slog.Info("fsynced to disk successfully")

	// Update status to COMMIT
	sc.Status = csstructs.COMMIT

	slog.Info("COMMIT SUCCESSFUL - data written to disk", "opID", sc.OpId, "chunkHandle", sc.ChunkHandle, "file", chunkFilePath, "bytesWritten", bytesWritten, "offset", sc.Offset)

	return nil
}

// Close cleans up resources if commit was not called
func (sc *StagedChunk) Close() {
	sc.mux.Lock()
	defer sc.mux.Unlock()

	if sc.tempFile != nil {
		tempPath := sc.tempFile.Name()
		sc.tempFile.Close()
		os.Remove(tempPath)
		sc.tempFile = nil
	}
	sc.buf = nil
}

func (sc *StagedChunk) Ready() {
	sc.mux.Lock()
	defer sc.mux.Unlock()

	sc.ready += 1
}

func (sc *StagedChunk) GetReadyCount() uint8 {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	return sc.ready
}

// IsQuorumReady checks if we have quorum (2 out of 3 replicas for RF=3)
func (sc *StagedChunk) IsQuorumReady(replicationFactor int) bool {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	// For RF=3, we need 2 replicas to confirm (quorum = majority)
	quorum := replicationFactor/2 + 1
	// ready count is the number of secondary replicas that confirmed
	// We need at least quorum-1 secondaries (since primary counts as 1)
	return int(sc.ready) >= (quorum - 1)
}
