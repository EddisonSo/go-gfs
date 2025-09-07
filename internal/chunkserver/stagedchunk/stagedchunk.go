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
	Offset uint64
}

func NewStagedChunk(chunkHandle string, opId string, size uint64, offset uint64) *StagedChunk {
	return &StagedChunk{
		ChunkHandle: chunkHandle,
		OpId: opId,
		buf: make([]byte, size),
		pos: 0,
		mux: sync.Mutex{},
		Offset: offset,
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
