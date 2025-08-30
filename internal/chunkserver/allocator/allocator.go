package allocator

import (
	"context"
	"errors"
	"time"
)

var (
	ErrSealed        = errors.New("chunk sealed")
	ErrDuplicateOpID = errors.New("duplicate op id")
	ErrUnknownOpID   = errors.New("unknown op id")
)

type ReserveReq struct {
	OpID     string
	Length   uint64
	NeedAcks int
	Resp     chan ReserveResp
}

type ReserveResp struct {
	Offset uint64
	Err    error
}

type ackMsg struct{ OpID string }
type commitMsg struct{ OpID string }
type abortMsg struct{ OpID string }
type sealMsg struct{}
type closeMsg struct{ done chan struct{} }

type Reservation struct {
	Offset   uint64
	Length   uint64
	Epoch    uint64
	State    StageState
	Acks     int
	NeedAcks int
	AddedAt  time.Time
}

type Allocator struct {
	capacity uint64
	tail     uint64
	epoch    uint64
	sealed   bool

	inflight map[string]Reservation // opId -> Reservation

	// inbox
	requests chan ReserveReq
	acks     chan ackMsg
	commits  chan commitMsg
	aborts   chan abortMsg
	seal     chan sealMsg
	closeCh  chan closeMsg
}

func NewAllocator(capacity uint64) *Allocator {
	a := &Allocator{
		capacity: capacity,
		inflight: make(map[string]Reservation),

		requests: make(chan ReserveReq, 1024),
		acks:     make(chan ackMsg, 1024),
		commits:  make(chan commitMsg, 1024),
		aborts:   make(chan abortMsg, 1024),
		seal:     make(chan sealMsg, 1),
		closeCh:  make(chan closeMsg, 1),
	}
	go a.run()
	return a
}

func (a *Allocator) Reserve(ctx context.Context, opID string, length uint64, needAcks int) (uint64, error) {
	respCh := make(chan ReserveResp, 1) 
	req := ReserveReq{OpID: opID, Length: length, NeedAcks: needAcks, Resp: respCh}
	select {
	case a.requests <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	select {
	case r := <-respCh:
		return r.Offset, r.Err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (a *Allocator) OnReplicaReady(opID string) { a.acks <- ackMsg{OpID: opID} }
func (a *Allocator) Commit(opID string) { a.commits <- commitMsg{OpID: opID} }
func (a *Allocator) Abort(opID string) { a.aborts <- abortMsg{OpID: opID} }
func (a *Allocator) Seal() { a.seal <- sealMsg{} }

func (a *Allocator) Close() {
	done := make(chan struct{})
	a.closeCh <- closeMsg{done: done}
	<-done
}

func (a *Allocator) run() {
	for {
		select {
		case req := <-a.requests:
			if a.sealed {
				req.Resp <- ReserveResp{0, ErrSealed}
				continue
			}
			if req.Length == 0 || req.Length > a.capacity {
				req.Resp <- ReserveResp{0, ErrChunkFull}
				continue
			}

			if _, exists := a.inflight[req.OpID]; exists {
				req.Resp <- ReserveResp{0, ErrDuplicateOpID}
				continue
			}

			if a.tail+req.Length > a.capacity {
				a.sealed = true
				req.Resp <- ReserveResp{0, ErrChunkFull}
				continue
			}

			offset := a.tail
			a.tail += req.Length
			a.epoch++ //monotonic epoch per reservation

			a.inflight[req.OpID] = Reservation{
				Offset:   offset,
				Length:   req.Length,
				Epoch:    a.epoch,
				State:    StageReserved,
				Acks:     0,
				NeedAcks: req.NeedAcks,
				AddedAt:  time.Now(),
			}
			req.Resp <- ReserveResp{offset, nil}

		case m := <-a.acks:
			res, ok := a.inflight[m.OpID]
			if !ok || a.sealed {
				continue
			}
			if res.State == StageReserved || res.State == StageReady {
				res.Acks++
				if res.Acks >= res.NeedAcks {
					res.State = StageReady
				}
				a.inflight[m.OpID] = res
			}

		case m := <-a.commits:
			res, ok := a.inflight[m.OpID]
			if !ok {
				continue
			}
			if res.State == StageReady || res.State == StageReserved {
				res.State = StageCommitted
				a.inflight[m.OpID] = res
				delete(a.inflight, m.OpID)
			}

		case m := <-a.aborts:
			res, ok := a.inflight[m.OpID]
			if !ok {
				continue
			}
			res.State = StageAborted
			a.inflight[m.OpID] = res

			if res.Offset+res.Length == a.tail {
				a.tail = res.Offset
			}
			delete(a.inflight, m.OpID)

		case <-a.seal:
			a.sealed = true

		case msg := <-a.closeCh:
			a.sealed = true
			close(msg.done)
			return
		}
	}
}

