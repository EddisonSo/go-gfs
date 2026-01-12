package gfs

import (
	pb "eddisonso.com/go-gfs/gen/master"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"github.com/golang-jwt/jwt/v5"
)

// SecretProvider supplies the JWT signing secret.
type SecretProvider func(*jwt.Token) (any, error)

// DefaultSecretProvider uses the built-in chunkserver secret provider.
var DefaultSecretProvider SecretProvider = secrets.GetSecret

// ReplicaPicker chooses a chunkserver for a read.
type ReplicaPicker func(chunk *pb.ChunkLocationInfo) *pb.ChunkServerInfo

// DefaultReplicaPicker prefers a replica location and falls back to primary.
func DefaultReplicaPicker(chunk *pb.ChunkLocationInfo) *pb.ChunkServerInfo {
	if chunk == nil {
		return nil
	}
	if len(chunk.Locations) > 0 {
		return chunk.Locations[0]
	}
	return chunk.Primary
}
