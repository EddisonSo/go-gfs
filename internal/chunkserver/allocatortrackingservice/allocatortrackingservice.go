package allocatortrackingservice

import (
	"eddisonso.com/go-gfs/internal/chunkserver/allocator"
	"sync"
	"errors"
)

var (
	ErrAllocatorNotFound = errors.New("Allocator not found")
)

type AllocatorTrackingService struct {
	allocators map[string]*allocator.Allocator //chunkHandle -> Allocator
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
			}
		}
	}
	return allocatorTrackingService
}

func (ats *AllocatorTrackingService) GetAllocator(chunkHandle string) (*allocator.Allocator, error) {
	if a, ok := ats.allocators[chunkHandle]; ok {
		return a, nil
	}
	return nil, ErrAllocatorNotFound
}

func (ats *AllocatorTrackingService) AddAllocator(c string, a *allocator.Allocator) {
	ats.allocators[c] = a
}
