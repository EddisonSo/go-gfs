package allocator

import (
	"sync"
)

type Allocator struct {
	offset        uint64
	chunksize     uint64
	writeSequence uint64
	mu            sync.Mutex
}

func NewAllocator(chunksize uint64) *Allocator {
	return &Allocator{
		offset: 0,
		chunksize: chunksize,
	}
}

// Allocate returns (offset, sequence, error) for an append operation
func (a *Allocator) Allocate(amount uint64) (uint64, uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	assignedOffset := a.offset
	if amount+a.offset > a.chunksize {
		return 0, 0, ErrInsufficientSpace
	}

	assignedSequence := a.writeSequence
	a.offset += amount
	a.writeSequence++
	return assignedOffset, assignedSequence, nil
}

// AllocateAt returns (sequence, error) for a random write at a specific offset.
// Updates the internal offset tracker if this write extends beyond current position.
func (a *Allocator) AllocateAt(offset uint64, amount uint64) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	endOffset := offset + amount
	if endOffset > a.chunksize {
		return 0, ErrInsufficientSpace
	}

	// Update offset tracker if this write extends the chunk
	if endOffset > a.offset {
		a.offset = endOffset
	}

	assignedSequence := a.writeSequence
	a.writeSequence++
	return assignedSequence, nil
}

