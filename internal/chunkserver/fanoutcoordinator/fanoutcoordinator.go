package fanoutcoordinator

import (
	"context"
	"net"

	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
)


type fanoutcoordinator struct {
	conn net.Conn
	replicas []csstructs.ReplicaIdentifier
	stagedchunk *stagedchunk.StagedChunk
}

func NewFanoutCoordinator(conn net.Conn) *fanoutcoordinator {
	return &fanoutcoordinator{conn: conn}
}

func (f *fanoutcoordinator) AddReplicas(replicas []csstructs.ReplicaIdentifier) {
	f.replicas = append(f.replicas,)
}

func (f *fanoutcoordinator) SetStagedChunk(stagedchunk *stagedchunk.StagedChunk) {
	f.stagedchunk = stagedchunk
}

func (f *fanoutcoordinator) StartFanout(ctx context.Context) error {
	return nil
}
