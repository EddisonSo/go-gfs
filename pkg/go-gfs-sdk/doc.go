// Package gfs provides a Go SDK for interacting with the Go-GFS filesystem.
//
// The SDK wraps master gRPC calls for metadata and uses the TCP dataplane for
// reading and appending chunk data.
package gfs
