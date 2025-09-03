package forwarder

import (
	"io"
	"log/slog"
	"strconv"

	"eddisonso.com/go-gfs/gen/chunkreplication"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"google.golang.org/grpc"
)

type Forwarder struct {
	replica csstructs.ReplicaIdentifier
	OpId string
	chunkHandle string
	Pr *io.PipeReader
	Pw *io.PipeWriter
	bufSize int64
}

func NewForwarder(replica csstructs.ReplicaIdentifier, opId string, chunkHandle string, bufSize int64) *Forwarder {
	pr, pw := io.Pipe()
	return &Forwarder{
		replica: replica,
		OpId: opId,
		chunkHandle: chunkHandle,
		Pr: pr,
		Pw: pw,
		bufSize: bufSize,
	}
}

func (f *Forwarder) StartForward() error {
	slog.Info("Starting forwarder", "replica", f.replica.Hostname, "opId", f.OpId, "chunkHandle", f.chunkHandle)

	conn, err := grpc.NewClient(f.replica.Hostname + ":" + strconv.Itoa(f.replica.ReplicationPort))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := chunkreplication.NewReplicatorClient(conn)

	buffer := make([]byte, f.bufSize)
	have := int64(0)

	for {
		n, err := f.Pr.Read(buffer[have:f.bufSize])
		if err != nil {
			if err == io.EOF {
				slog.Info("Forwarder reached EOF", "replica", f.replica.Hostname, "opId", f.OpId, "chunkHandle", f.chunkHandle)
				return nil
			}
			slog.Error("Error reading from pipe", "error", err, "replica", f.replica.Hostname, "opId", f.OpId, "chunkHandle", f.chunkHandle)
			return err
		}

		have += int64(n)
		if have == f.bufSize {
			//flush
		}


	}
}
