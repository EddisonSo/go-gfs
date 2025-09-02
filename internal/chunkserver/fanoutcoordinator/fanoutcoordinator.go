package fanoutcoordinator

import (
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/forwarder"
	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
)


type fanoutcoordinator struct {
	replicas []csstructs.ReplicaIdentifier
	stagedchunk *stagedchunk.StagedChunk
}

func NewFanoutCoordinator(replicas []csstructs.ReplicaIdentifier, stagedchunk *stagedchunk.StagedChunk) *fanoutcoordinator {
	return &fanoutcoordinator{
		replicas: replicas,
		stagedchunk: stagedchunk,
	}
}

func (f *fanoutcoordinator) AddReplicas(replicas []csstructs.ReplicaIdentifier) {
	f.replicas = append(f.replicas,)
}

func (f *fanoutcoordinator) SetStagedChunk(stagedchunk *stagedchunk.StagedChunk) {
	f.stagedchunk = stagedchunk
}

func (f *fanoutcoordinator) StartFanout(conn net.Conn, jwtTokenString string) error {
	forwarders := make([]*forwarder.Forwarder, len(f.replicas))

	jwtLength := len(jwtTokenString)
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(jwtLength))

	slog.Info("Starting fanout", "replicas", f.replicas)
	
	for i, replica := range f.replicas {
		forwarders[i] = forwarder.NewForwarder(replica, f.stagedchunk.OpId, f.stagedchunk.ChunkHandle)
		go forwarders[i].StartForward()

		forwarders[i].Pw.Write(lengthBytes)
		forwarders[i].Pw.Write([]byte(jwtTokenString))
	}

	buf := make([]byte, 64<<10)
	total := 0
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			total += n
			f.stagedchunk.Read(buf[:n])

			for _, fw := range forwarders {
				fw.Pw.Write(buf[:n])
			}
		}
		if err != nil {
			for _, fw := range forwarders {
				if err := fw.Pw.Close(); err != nil {
					slog.Error("Failed to close forwarder pipe", "error", err)
				}
			}
		}
		if err == io.EOF {
			break
		}
		return err
	}
    
	return nil
}
