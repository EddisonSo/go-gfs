package forwarder

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"

	pb "eddisonso.com/go-gfs/gen/chunkreplication"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/stagedchunk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Forwarder struct {
	replica csstructs.ReplicaIdentifier
	opId string
	chunkHandle string
	stagedchunk *stagedchunk.StagedChunk
	Lr *io.LimitedReader
	Pw *io.PipeWriter
	chunkSize uint64
	offset uint64
}

func NewForwarder(replica csstructs.ReplicaIdentifier, opId string, chunkHandle string, stagedchunk *stagedchunk.StagedChunk, chunkSize uint64, offset uint64) *Forwarder {
	pr, pw := io.Pipe()
	lr := io.LimitedReader{R:pr, N:int64(chunkSize)}
	return &Forwarder{
		replica: replica,
		opId: opId,
		chunkHandle: chunkHandle,
		stagedchunk: stagedchunk,
		Lr: &lr,
		Pw: pw,
		chunkSize: chunkSize,
		offset: offset,
	}
}

func (f *Forwarder) StartForward() error {
	slog.Info("Starting forwarder", "replica", f.replica.Hostname, "opId", f.opId, "chunkHandle", f.chunkHandle)

	conn, err := grpc.NewClient(f.replica.Hostname + ":" + strconv.Itoa(f.replica.ReplicationPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("Failed to connect to replica", "replica", f.replica.Hostname, "opId", f.opId, "chunkHandle", f.chunkHandle, "error", err)
		return err
	}
	defer conn.Close()
	client := pb.NewReplicatorClient(conn)

	ctx := context.TODO()
	stream, err := client.Replicate(ctx)
	if err != nil {
		slog.Error("Failed to create replication stream", "replica", f.replica.Hostname, "opId", f.opId, "chunkHandle", f.chunkHandle, "error", err)
		return err
	}

	slog.Info("Forwarder connected to replica", "replica", f.replica.Hostname, "opId", f.opId, "chunkHandle", f.chunkHandle)

	meta := &pb.ReplicationMetadata{
		OpId: f.opId,
		ChunkHandle: f.chunkHandle,
		Length: f.chunkSize,
		Offset: f.offset,
		Epoch: 1,
	}


	err = stream.Send(&pb.ReplicationFrame{
		Kind: &pb.ReplicationFrame_Meta{Meta: meta},
	})

	if err != nil {
		return err
	}

	slog.Info("Sent metadata to replica", "replica", f.replica.Hostname, "opId", f.opId, "chunkHandle", f.chunkHandle, "length", f.chunkSize)
	
	buf := make([]byte, 1 << 20)
	var currBytes uint64 = 0
	var totalBytes uint64 = 0

	for {
		n, err := f.Lr.Read(buf[currBytes:])
		totalBytes += uint64(n)
		currBytes += uint64(n)

		if err == io.EOF {
			if totalBytes != f.chunkSize {
				return errors.New("forwarder: read less bytes than expected: " + strconv.FormatUint(currBytes, 10) + " < " + strconv.FormatUint(f.chunkSize, 10))
			}
			if currBytes > 0 {
				replicationData := pb.ReplicationData{Data: buf[:currBytes], Seq: 0}
				err = stream.Send(&pb.ReplicationFrame{
					Kind: &pb.ReplicationFrame_Data{Data: &replicationData},
				})

				if err != nil {
					slog.Error("Failed to send final data to replica", "replica", f.replica.Hostname, "opId", f.opId, "chunkHandle", f.chunkHandle, "error", err)
					return err
				}
				slog.Info("Finished data transfer to replica", "replica", f.replica.Hostname, "opId", f.opId, "chunkHandle", f.chunkHandle, "n", n, "totalBytes", totalBytes)
			}
			stream.CloseAndRecv()
			f.stagedchunk.Ready()
			return nil
		}

		if err != nil {
			return err
		}

		if currBytes == uint64(cap(buf)) {
			replicationData := pb.ReplicationData{Data: buf, Seq: 0}
			err = stream.Send(&pb.ReplicationFrame{
				Kind: &pb.ReplicationFrame_Data{Data: &replicationData},
			})

			if err != nil {
				return err
			}

			currBytes = 0
			buf = make([]byte, 1 << 20)
		}
	}
}
