package fanoutcoordinator

import (
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
	expectedSize := f.stagedchunk.Cap()

	slog.Info("Starting fanout", "replicas", f.replicas, "expectedSize", expectedSize)

	for i, replica := range f.replicas {
		forwarders[i] = forwarder.NewForwarder(replica, f.stagedchunk.OpId, f.stagedchunk.ChunkHandle, f.stagedchunk, f.stagedchunk.Cap(), f.stagedchunk.Offset, f.stagedchunk.Sequence)
		go forwarders[i].StartForward()
	}

	// Use LimitReader to read exactly expectedSize bytes
	// This allows connection reuse (client doesn't need to call CloseWrite)
	reader := io.LimitReader(conn, int64(expectedSize))
	buf := make([]byte, 64<<10)
	total := 0
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			total += n
			if _, writeErr := f.stagedchunk.Write(buf[:n]); writeErr != nil {
				slog.Error("failed to write to staged chunk", "error", writeErr, "totalBytes", total)
				for _, fw := range forwarders {
					fw.Pw.Close()
				}
				return writeErr
			}

			for i, fw := range forwarders {
				if _, writeErr := fw.Pw.Write(buf[:n]); writeErr != nil {
					slog.Error("failed to write to forwarder", "index", i, "error", writeErr)
					// Continue writing to other forwarders - quorum may still succeed
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				slog.Info("finished reading data from client", "totalBytes", total, "expectedSize", expectedSize)
				// Close forwarders
				for _, fw := range forwarders {
					if err := fw.Pw.Close(); err != nil {
						slog.Error("Failed to close forwarder pipe", "error", err)
					}
				}
				break
			}
			// Non-EOF error - close and return error
			slog.Error("error reading from client", "error", err)
			for _, fw := range forwarders {
				fw.Pw.Close()
			}
			return err
		}
	}

	slog.Info("fanout completed successfully", "totalBytes", total)
	return nil
}
