package stagedchunk

import (
	"io"
	"bytes"
	"sync"
)

type StagedChunk struct {
	ChunkHandle string
	OpId string
	buf []byte
	pos int
	mux sync.Mutex
}

func NewStagedChunk(chunkHandle string, opId string, size int64) *StagedChunk {
	return &StagedChunk{
		ChunkHandle: chunkHandle,
		OpId: opId,
		buf: make([]byte, 0, size),
		pos: 0,
		mux: sync.Mutex{},
	}
}

func (sc *StagedChunk) Read(p []byte) (int, error) {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	if sc.pos >= len(sc.buf) {
		return 0, io.EOF
	}

	n := copy(p, sc.buf[sc.pos:])
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

func (sc *StagedChunk) Len() int {
	sc.mux.Lock()
	defer sc.mux.Unlock()
	return len(sc.buf)
}
