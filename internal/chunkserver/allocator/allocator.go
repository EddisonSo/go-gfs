package allocator

import (
	"sync"
)

type Allocator struct {
	offset uint64
	chunksize uint64
	mu sync.Mutex
}

func NewAllocator(chunksize uint64) *Allocator {
	return &Allocator{
		offset: 0,
		chunksize: chunksize,
	}
}

//Returns offset that append is allocated to 
func (a *Allocator) Allocate(amount uint64) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	assignedOffset := a.offset
	if amount + a.offset > a.chunksize {
		return 0, ErrInsufficientSpace
	}

	a.offset += amount
	return assignedOffset, nil
}

