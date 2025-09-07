package allocator

import (
	"errors"
)

type Chunkallocation struct {
	Chunkhandle string
	CurrOffset uint64
}


var (
	ErrInsufficientSpace = errors.New("Insufficient space in chunk")
)
