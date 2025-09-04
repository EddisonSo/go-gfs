package replicationplane

import (
	"io"
	"errors"

	pb "eddisonso.com/go-gfs/gen/chunkreplication"
	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
	"eddisonso.com/go-gfs/internal/chunkserver/chunkstagingtrackingservice"
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
			// finalize / success response
			msg := "replication accepted"
			if chunkHandle != "" {
				msg += " for " + chunkHandle
			}
			return stream.SendAndClose(&pb.ReplicationResponse{
				Success: true,
				Message: msg,
			})
		}

		switch v := frame.GetKind().(type) {
		case *pb.ReplicationFrame_Meta:
			meta := v.Meta
			chunkHandle = meta.GetChunkHandle()
			opID = meta.GetOpId()
			length = meta.GetLength()

			sc = stagedchunk.NewStagedChunk(chunkHandle, opID, length)
			chunkstagingtrackingservice.GetChunkStagingTrackingService().AddStagedChunk(sc)

		case *pb.ReplicationFrame_Data:
			if sc == nil {
				return errors.New("DATA before META")
			}

			data := v.Data.GetData()
			sc.Read(data)
		default:
			return errors.New("unknown frame kind")
		}
	}
}
