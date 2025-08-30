package allocator

import (
	"errors"
)

type StageState int
const (
	StageReserved StageState = iota
	StageReady
	StageCommitted
	StageAborted
	StageSealed
)

var (
	ErrChunkFull = errors.New("chunk is full")
	ErrNotFound  = errors.New("item not found")
)
