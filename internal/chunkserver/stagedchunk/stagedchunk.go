package stagedchunk

import (
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"io"
	"bytes"
	"sync"
)

type StagedChunk struct {
	ChunkHandle string
	OpId string
	Status csstructs.StageState
	buf []byte
	pos int
	mux sync.Mutex
}

func NewStagedChunk(chunkHandle string, opId string, status csstructs.StageState, size int64) *StagedChunk {
	return &StagedChunk{
		ChunkHandle: chunkHandle,
		OpId: opId,
		Status: status,
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
	return bytes.NewReader(sc.buf)
}

func (sc *StagedChunk) Bytes() []byte {
	return sc.buf
}

func (sc *StagedChunk) Len() int {
	return len(sc.buf)
}
