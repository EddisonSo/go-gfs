package forwarder

import (
	"io"
	"log/slog"
	"net"

	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
)

type Forwarder struct {
	replica csstructs.ReplicaIdentifier
	OpId string
	chunkHandle string
	Pr *io.PipeReader
	Pw *io.PipeWriter
	idx int
}

func NewForwarder(replica csstructs.ReplicaIdentifier, opId string, chunkHandle string) *Forwarder {
	pr, pw := io.Pipe()
	return &Forwarder{
		replica: replica,
		OpId: opId,
		chunkHandle: chunkHandle,
		Pr: pr,
		Pw: pw,
		idx: 0,
	}
}

func (f *Forwarder) StartForward() error {
	slog.Info("Starting forwarder", "replica", f.replica.Hostname, "opId", f.OpId, "chunkHandle", f.chunkHandle)
	replicaConn, err := net.Dial("tcp", f.replica.Hostname)
	if err != nil {
		slog.Error("Failed to connect to replica", "replica", f.replica.Hostname, "error", err)
		return err
	}
	defer replicaConn.Close()
	slog.Info("Connected to replica", "replica", f.replica.Hostname)

	_, err = io.Copy(replicaConn, f.Pr)
	if err != nil {
		slog.Error("Failed to forward data", "replica", f.replica.Hostname, "error", err)
		return err
	}
	return nil
}
