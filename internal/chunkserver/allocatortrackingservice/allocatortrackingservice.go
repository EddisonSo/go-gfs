package allocatortrackingservice

import (
	"eddisonso.com/go-gfs/internal/chunkserver/allocator"
	"sync"
)

type AllocatorTrackingService struct {
	allocators map[string]*allocator.Allocator
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

func (ats *AllocatorTrackingService) GetAllocator(opId string) *allocator.Allocator {
	return ats.allocators[opId]
}
