package gfs

import "errors"

var (
	// ErrNoChunkLocations indicates the master returned no chunk locations.
	ErrNoChunkLocations = errors.New("no chunk locations available")
	// ErrNoPrimary indicates the chunk has no primary for writes.
	ErrNoPrimary = errors.New("chunk has no primary")
	// ErrNoReplica indicates no replica could be selected for reads.
	ErrNoReplica = errors.New("no replica available for read")
	// ErrInvalidOffset indicates the provided offset is invalid.
	ErrInvalidOffset = errors.New("invalid offset")
)
