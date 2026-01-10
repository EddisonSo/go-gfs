package replicationplane

import (
	"net"
	"strconv"
	"google.golang.org/grpc"
	"log/slog"
	pb "eddisonso.com/go-gfs/gen/chunkreplication"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
)

type ReplicationServer struct {
	config csstructs.ChunkServerConfig
}

func NewReplicationServer(config csstructs.ChunkServerConfig) *ReplicationServer {
	return &ReplicationServer{
		config: config,
	}
}

func (rs *ReplicationServer) Start() {
	lis, _ := net.Listen("tcp", ":" + strconv.Itoa(rs.config.ReplicationPort))
	slog.Info("Starting Replication Server on port " + strconv.Itoa(rs.config.ReplicationPort))
	grpcServer := grpc.NewServer()
	pb.RegisterReplicatorServer(grpcServer, NewReplicationPlane(rs.config))
	grpcServer.Serve(lis)
}
