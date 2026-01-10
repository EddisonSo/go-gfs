package replicationclient

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pb "eddisonso.com/go-gfs/gen/chunkreplication"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SendCommitToReplica sends a COMMIT message to a single replica
func SendCommitToReplica(replica csstructs.ReplicaIdentifier, opID string) error {
	addr := fmt.Sprintf("%s:%d", replica.Hostname, replica.ReplicationPort)

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	client := pb.NewReplicatorClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.Commit{
		OpId: opID,
	}

	slog.Info("sending commit to replica", "replica", replica.ID, "opID", opID)

	resp, err := client.RecvCommit(ctx, req)
	if err != nil {
		return fmt.Errorf("RPC failed for %s: %w", replica.ID, err)
	}

	if !resp.Success {
		return fmt.Errorf("commit failed on %s: %s", replica.ID, resp.Message)
	}

	slog.Info("commit successful on replica", "replica", replica.ID, "opID", opID)
	return nil
}

// SendCommitToAllReplicas sends COMMIT to all replicas and returns errors for any failures
func SendCommitToAllReplicas(replicas []csstructs.ReplicaIdentifier, opID string) []error {
	errors := make([]error, 0)

	for _, replica := range replicas {
		if err := SendCommitToReplica(replica, opID); err != nil {
			slog.Error("failed to commit on replica", "replica", replica.ID, "error", err)
			errors = append(errors, err)
		}
	}

	return errors
}
