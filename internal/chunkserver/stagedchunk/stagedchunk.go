package stagedchunk

import (
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"io"
	"bytes"
)

type StagedChunk struct {
	ChunkHandle string
	OpId string
	Status csstructs.StageState
	buf []byte
	pos int
}

func NewStagedChunk(chunkHandle string, opId string, status csstructs.StageState, data []byte) *StagedChunk {
	return &StagedChunk{
		ChunkHandle: chunkHandle,
		OpId: opId,
		Status: status,
		buf: data,
	}
}

func (sc *StagedChunk) Read(p []byte) (int, error) {
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
