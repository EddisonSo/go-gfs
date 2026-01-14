package allocatortrackingservice

import (
	"eddisonso.com/go-gfs/internal/chunkserver/allocator"
	"errors"
	"sync"
)

var (
	ErrAllocatorNotFound = errors.New("Allocator not found")
)

type AllocatorTrackingService struct {
	allocators map[string]*allocator.Allocator //chunkHandle -> Allocator
	writeLocks map[string]*sync.Mutex          //chunkHandle -> per-chunk write lock
	mu         sync.Mutex                      // protects maps
}

var lock = &sync.Mutex{}
var allocatorTrackingService *AllocatorTrackingService

func GetAllocatorTrackingService() *AllocatorTrackingService {
	if allocatorTrackingService == nil {
		lock.Lock()
		defer lock.Unlock()
		if allocatorTrackingService == nil {
			allocatorTrackingService = &AllocatorTrackingService{
				allocators: make(map[string]*allocator.Allocator),
				writeLocks: make(map[string]*sync.Mutex),
			}
		}
	}
	return allocatorTrackingService
}

func (ats *AllocatorTrackingService) GetAllocator(chunkHandle string) (*allocator.Allocator, error) {
	ats.mu.Lock()
	defer ats.mu.Unlock()
	if a, ok := ats.allocators[chunkHandle]; ok {
		return a, nil
	}
	return nil, ErrAllocatorNotFound
}

func (ats *AllocatorTrackingService) AddAllocator(c string, a *allocator.Allocator) {
	ats.mu.Lock()
	defer ats.mu.Unlock()
	ats.allocators[c] = a
}

// AcquireWriteLock acquires the per-chunk write lock.
// This serializes all writes to a chunk, ensuring no sequence gaps.
// Must call ReleaseWriteLock when done.
func (ats *AllocatorTrackingService) AcquireWriteLock(chunkHandle string) {
	ats.mu.Lock()
	wl, ok := ats.writeLocks[chunkHandle]
	if !ok {
		wl = &sync.Mutex{}
		ats.writeLocks[chunkHandle] = wl
	}
	ats.mu.Unlock()

	wl.Lock()
}

// ReleaseWriteLock releases the per-chunk write lock.
func (ats *AllocatorTrackingService) ReleaseWriteLock(chunkHandle string) {
	ats.mu.Lock()
	wl, ok := ats.writeLocks[chunkHandle]
	ats.mu.Unlock()

	if ok {
		wl.Unlock()
	}
}
