package allocator

import (
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"time"
)

type ReserveReq struct {
	OpID string
	Length uint64
	Resp chan ReserveResp
}

type ReserveResp struct {
	Offset uint64
	Err error
}

type Reservation struct {
	Offset uint64
	Length uint64
	Epoch uint64
	State csstructs.StageState
	Acks int
	NeedAcks int
	AddedAt time.Time
}

type Allocator struct {
	capacity uint64
	tail uint64
	inflight map[string]Reservation // opId -> {offset,len}
	requests chan ReserveReq
}
