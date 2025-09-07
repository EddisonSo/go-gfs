package replicationplane

import (
	"errors"
	"io"
	"log/slog"

	pb "eddisonso.com/go-gfs/gen/chunkreplication"
	"eddisonso.com/go-gfs/internal/chunkserver/chunkstagingtrackingservice"
	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
)

type ReplicationPlane struct {
	pb.UnimplementedReplicatorServer
}


func (rp *ReplicationPlane) Replicate(stream pb.Replicator_ReplicateServer) error {
	var sc *stagedchunk.StagedChunk
	var chunkHandle, opID string
	var length uint64

	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			msg := "replication accepted"
			if chunkHandle != "" {
				msg += " for " + chunkHandle
			}

			slog.Info("replication complete", "chunkHandle", chunkHandle, "opID", opID, "length", length)
			return stream.SendAndClose(&pb.ReplicationResponse{
				Success: true,
				Message: msg,
			})
		}

		switch v := frame.GetKind().(type) {
		case *pb.ReplicationFrame_Meta:
			slog.Info(frame.String())
			meta := v.Meta
			chunkHandle = meta.GetChunkHandle()
			opID = meta.GetOpId()
			length = meta.GetLength()

			slog.Info("starting replication", "chunkHandle", chunkHandle, "opID", opID, "length", length)

			sc = stagedchunk.NewStagedChunk(chunkHandle, opID, length)
			chunkstagingtrackingservice.GetChunkStagingTrackingService().AddStagedChunk(sc)
		case *pb.ReplicationFrame_Data:
			if sc == nil {
				return errors.New("DATA before META")
			}

			data := v.Data.GetData()
			_, err := sc.Write(data)
			if err != nil {
				slog.Error("error writing data to staged chunk", "chunkHandle", chunkHandle, "opID", opID, "error", err)
				return err
			}
		default:
			slog.Info("unknown replication frame kind", "chunkHandle", chunkHandle, "opID", opID)
			return errors.New("unknown frame kind")
		}
	}
}
