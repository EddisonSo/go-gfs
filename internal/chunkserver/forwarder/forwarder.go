package forwarder

import (
	"io"
	"log/slog"
	"errors"
	"strconv"

	pb "eddisonso.com/go-gfs/gen/chunkreplication"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"google.golang.org/grpc"
)

type Forwarder struct {
	replica csstructs.ReplicaIdentifier
	OpId string
	chunkHandle string
	Pr *io.PipeReader
	Pw *io.PipeWriter
	chunkSize uint64
}

func NewForwarder(replica csstructs.ReplicaIdentifier, opId string, chunkHandle string, chunkSize uint64) *Forwarder {
	pr, pw := io.Pipe()
	return &Forwarder{
		replica: replica,
		OpId: opId,
		chunkHandle: chunkHandle,
		Pr: pr,
		Pw: pw,
		chunkSize: chunkSize,
	}
}

func (f *Forwarder) StartForward() error {
	slog.Info("Starting forwarder", "replica", f.replica.Hostname, "opId", f.OpId, "chunkHandle", f.chunkHandle)

	conn, err := grpc.NewClient(f.replica.Hostname + ":" + strconv.Itoa(f.replica.ReplicationPort))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewReplicatorClient(conn)

	stream, err := client.Replicate(nil)
	if err != nil {
		return err
	}

	meta := &pb.ReplicationMetadata{
		OpId: f.OpId,
		ChunkHandle: f.chunkHandle,
		Length: f.chunkSize,
		Epoch: 1,
	}


	err = stream.Send(&pb.ReplicationFrame{
		Kind: &pb.ReplicationFrame_Meta{Meta: meta},
	})

	if err != nil {
		return err
	}
	
	buf := make([]byte, 1 << 20)
	var currBytes uint64 = 0
	var totalBytes uint64 = 0

	for {
		n, err := f.Pr.Read(buf)
		totalBytes += uint64(n)
		currBytes += uint64(n)

		if err == io.EOF {
			if totalBytes != f.chunkSize {
				return errors.New("forwarder: read less bytes than expected: " + strconv.FormatUint(currBytes, 10) + " < " + strconv.FormatUint(f.chunkSize, 10))
			}
			if currBytes > 0 {
				replicationData := pb.ReplicationData{Data: buf, Seq: 0}
				err = stream.Send(&pb.ReplicationFrame{
					Kind: &pb.ReplicationFrame_Data{Data: &replicationData},
				})

				if err != nil {
					return err
				}
			}
			return nil
		}

		if err != nil {
			return err
		}

		if currBytes == f.chunkSize {
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
