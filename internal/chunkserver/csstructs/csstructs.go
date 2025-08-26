package csstructs

import (
	"github.com/golang-jwt/jwt/v5"
)

type ChunkServerConfig struct {
	Hostname string
	DataPort int
	ReplicationPort int
	Id 	 string
	Dir 	 string
}

type ReplicaIdentifier struct {
	ID   string `json:"id"`
	Hostname string `json:"hostname"`
	DataPort int
	ReplicationPort int
}

type DownloadRequestClaims struct {
	ChunkHandle string `json:"chunk_handle"`
	Operation string `json:"operation"`
	Filesize int64 `json:"file_size"`
	Replicas []ReplicaIdentifier `json:"replicas"`
	Primary ReplicaIdentifier `json:"primary"`
	jwt.RegisteredClaims
}

type StageState int
const (
	Receiving StageState = iota
	Staged
	Applied
	Aborted
)

type Action int
const (
	Download Action = iota
	Upload
)
