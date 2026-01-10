package stagedchunk

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
)

type StagedChunk struct {
	ChunkHandle string
	OpId string
	buf []byte
	pos int
	mux sync.Mutex
	Offset uint64
	Status csstructs.Status
	ready uint8
	storageDir string
}

func NewStagedChunk(chunkHandle string, opId string, size uint64, offset uint64, storageDir string) *StagedChunk {
	return &StagedChunk{
		ChunkHandle: chunkHandle,
		OpId: opId,
		buf: make([]byte, size),
		pos: 0,
		mux: sync.Mutex{},
		Offset: offset,
		Status: csstructs.READY,
		ready: 0,
		storageDir: storageDir,
	}
}

func (sc *StagedChunk) Write(p []byte) (int, error) {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	if sc.pos >= cap(sc.buf) {
		return 0, io.EOF
	}

	n := copy(sc.buf[sc.pos:], p)
	sc.pos += n

	return n, nil
}

func (sc *StagedChunk) NewReader() io.Reader {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	return bytes.NewReader(sc.buf)
}

func (sc *StagedChunk) Bytes() []byte {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	cp := make([]byte, len(sc.buf))
	copy(cp, sc.buf)
	return cp
}

func (sc *StagedChunk) Len() uint64 {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	return uint64(len(sc.buf))
}

func (sc *StagedChunk) Cap() uint64 {
	return uint64(cap(sc.buf))
}

func (sc *StagedChunk) Pos() uint64 {
	return uint64(sc.pos)
}

func (sc *StagedChunk) Commit() error {
	sc.mux.Lock()
	defer sc.mux.Unlock()

	slog.Info("starting commit", "opID", sc.OpId, "chunkHandle", sc.ChunkHandle, "offset", sc.Offset, "size", sc.pos, "storageDir", sc.storageDir)

	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(sc.storageDir, 0755); err != nil {
		slog.Error("failed to create storage directory", "dir", sc.storageDir, "error", err)
		return fmt.Errorf("failed to create storage directory: %w", err)
	}
	slog.Info("storage directory ready", "dir", sc.storageDir)

	// Chunk file path
	chunkFilePath := filepath.Join(sc.storageDir, sc.ChunkHandle)
	slog.Info("writing to file", "path", chunkFilePath)

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
	slog.Info("seeked to offset", "offset", sc.Offset)

	// Write the buffer to disk
	bytesWritten, err := file.Write(sc.buf[:sc.pos])
	if err != nil {
		slog.Error("failed to write data", "error", err)
		return fmt.Errorf("failed to write data to disk: %w", err)
	}
	slog.Info("wrote data to disk", "bytes", bytesWritten)

	if bytesWritten != sc.pos {
		slog.Error("incomplete write", "wrote", bytesWritten, "expected", sc.pos)
		return fmt.Errorf("incomplete write: wrote %d bytes, expected %d", bytesWritten, sc.pos)
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
