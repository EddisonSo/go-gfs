package gfs

import (
	"context"
	"sync"
	"time"

	pb "eddisonso.com/go-gfs/gen/master"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fileKey identifies a file in the chunk cache.
type fileKey struct {
	namespace string
	path      string
}

// chunkCache holds cached chunk locations for a single file.
type chunkCache struct {
	chunks []*pb.ChunkLocationInfo
	mu     sync.RWMutex
}

const defaultChunkTimeout = 120 * time.Second
const defaultMaxChunkSize = int64(64 << 20)
const defaultReadConcurrency = 3

// Option configures the SDK client.
type Option func(*clientConfig)

type clientConfig struct {
	dialOptions     []grpc.DialOption
	chunkTimeout    time.Duration
	maxChunkSize    int64
	readConcurrency int
	secretProvider  SecretProvider
	replicaPicker   ReplicaPicker
}

// New creates a new SDK client connected to the master gRPC endpoint.
func New(ctx context.Context, masterAddr string, opts ...Option) (*Client, error) {
	cfg := clientConfig{
		dialOptions:     []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		chunkTimeout:    defaultChunkTimeout,
		maxChunkSize:    defaultMaxChunkSize,
		readConcurrency: defaultReadConcurrency,
		secretProvider:  DefaultSecretProvider,
		replicaPicker:   DefaultReplicaPicker,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	conn, err := grpc.DialContext(ctx, masterAddr, cfg.dialOptions...)
	if err != nil {
		return nil, err
	}

	return &Client{
		masterAddr:      masterAddr,
		conn:            conn,
		master:          pb.NewMasterClient(conn),
		chunkTimeout:    cfg.chunkTimeout,
		maxChunkSize:    cfg.maxChunkSize,
		readConcurrency: cfg.readConcurrency,
		secretProvider:  cfg.secretProvider,
		replicaPicker:   cfg.replicaPicker,
		chunkCache:      make(map[fileKey]*chunkCache),
	}, nil
}

// Client is the SDK entry point for interacting with Go-GFS.
type Client struct {
	masterAddr      string
	conn            *grpc.ClientConn
	master          pb.MasterClient
	chunkTimeout    time.Duration
	maxChunkSize    int64
	readConcurrency int
	secretProvider  SecretProvider
	replicaPicker   ReplicaPicker

	chunkCacheMu sync.RWMutex
	chunkCache   map[fileKey]*chunkCache
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// WithDialOptions overrides the gRPC dial options used to connect to master.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(cfg *clientConfig) {
		cfg.dialOptions = opts
	}
}

// WithChunkTimeout sets the per-chunk timeout for dataplane operations.
func WithChunkTimeout(timeout time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.chunkTimeout = timeout
	}
}

// WithMaxChunkSize sets the maximum chunk size used by the client.
func WithMaxChunkSize(size int64) Option {
	return func(cfg *clientConfig) {
		cfg.maxChunkSize = size
	}
}

// WithSecretProvider overrides the JWT signing secret provider.
func WithSecretProvider(provider SecretProvider) Option {
	return func(cfg *clientConfig) {
		if provider != nil {
			cfg.secretProvider = provider
		}
	}
}

// WithReplicaPicker overrides the replica selection strategy for reads.
func WithReplicaPicker(picker ReplicaPicker) Option {
	return func(cfg *clientConfig) {
		if picker != nil {
			cfg.replicaPicker = picker
		}
	}
}

// WithReadConcurrency sets the maximum number of concurrent chunk reads.
func WithReadConcurrency(n int) Option {
	return func(cfg *clientConfig) {
		if n > 0 {
			cfg.readConcurrency = n
		}
	}
}
