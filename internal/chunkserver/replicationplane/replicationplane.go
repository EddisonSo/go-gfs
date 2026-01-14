package replicationplane

import (
	"context"
	"errors"
	"io"
	"log/slog"

	pb "eddisonso.com/go-gfs/gen/chunkreplication"
	"eddisonso.com/go-gfs/internal/chunkserver/chunkstagingtrackingservice"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
)

type ReplicationPlane struct {
	pb.UnimplementedReplicatorServer
	config csstructs.ChunkServerConfig
}

func NewReplicationPlane(config csstructs.ChunkServerConfig) *ReplicationPlane {
	return &ReplicationPlane{
		config: config,
	}
}


func (rp *ReplicationPlane) Replicate(stream pb.Replicator_ReplicateServer) error {
	var sc *stagedchunk.StagedChunk
	var chunkHandle, opID string
	var length uint64
	var offset uint64
	var sequence uint64

	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			msg := "replication accepted"
			if chunkHandle != "" {
				msg += " for " + chunkHandle
			}

			slog.Info("replication complete", "chunkHandle", chunkHandle, "opID", opID, "length", length, "sequence", sequence)
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
			offset = meta.GetOffset()
			sequence = meta.GetSequence()

			slog.Info("starting replication", "chunkHandle", chunkHandle, "opID", opID, "length", length, "offset", offset, "sequence", sequence)

			sc = stagedchunk.NewStagedChunk(chunkHandle, opID, length, offset, sequence, rp.config.Dir)
			if sc == nil {
				return errors.New("failed to create staged chunk")
			}
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

func (rp *ReplicationPlane) RecvReady(ctx context.Context, req *pb.Ready) (*pb.ReplicationResponse, error) {
	opID := req.GetOpId()
	replica := req.GetReplica()

	slog.Info("received ready signal", "opID", opID, "replica", replica)

	// Get the staged chunk by operation ID
	trackingService := chunkstagingtrackingservice.GetChunkStagingTrackingService()
	sc := trackingService.GetStagedChunk(opID)

	if sc == nil {
		slog.Error("staged chunk not found", "opID", opID)
		return &pb.ReplicationResponse{
			Success: false,
			Message: "staged chunk not found for opID: " + opID,
		}, nil
	}

	// Mark the replica as ready
	sc.Ready()

	slog.Info("staged chunk marked as ready", "opID", opID, "chunkHandle", sc.ChunkHandle)

	return &pb.ReplicationResponse{
		Success: true,
		Message: "ready acknowledged",
	}, nil
}

func (rp *ReplicationPlane) RecvCommit(ctx context.Context, req *pb.Commit) (*pb.ReplicationResponse, error) {
	opID := req.GetOpId()

	slog.Info("received commit signal", "opID", opID)

	// Get the staged chunk by operation ID
	trackingService := chunkstagingtrackingservice.GetChunkStagingTrackingService()
	sc := trackingService.GetStagedChunk(opID)

	if sc == nil {
		slog.Error("staged chunk not found", "opID", opID)
		return &pb.ReplicationResponse{
			Success: false,
			Message: "staged chunk not found for opID: " + opID,
		}, nil
	}

	// With per-chunk serialization on the primary, commits arrive in order
	// No need for sequence-based ordering - commit directly
	if err := sc.Commit(); err != nil {
		slog.Error("failed to commit staged chunk", "opID", opID, "chunkHandle", sc.ChunkHandle, "error", err)
		return &pb.ReplicationResponse{
			Success: false,
			Message: "failed to commit: " + err.Error(),
		}, nil
	}

	slog.Info("commit applied", "opID", opID, "chunkHandle", sc.ChunkHandle, "sequence", sc.Sequence)

	return &pb.ReplicationResponse{
		Success: true,
		Message: "commit successful",
	}, nil
}
