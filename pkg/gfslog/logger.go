package gfslog

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	pb "eddisonso.com/go-gfs/gen/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	bufferSize       = 1000
	reconnectBackoff = 5 * time.Second
	sendTimeout      = 5 * time.Second
)

// Logger wraps slog.Logger with remote log service support.
type Logger struct {
	*slog.Logger
	conn     *grpc.ClientConn
	client   pb.LogServiceClient
	entryCh  chan *pb.LogEntry
	cancel   context.CancelFunc
	mu       sync.Mutex
	addr     string
	source   string
	minLevel slog.Level
}

// Config holds configuration for creating a new Logger.
type Config struct {
	Source         string
	LogServiceAddr string
	MinLevel       slog.Level
}

// NewLogger creates a new Logger that sends logs to both stdout and the log service.
// If logServiceAddr is empty, only stdout logging is used.
func NewLogger(cfg Config) *Logger {
	entryCh := make(chan *pb.LogEntry, bufferSize)

	handler := NewHandler(cfg.Source, cfg.MinLevel, entryCh)

	// Create stdout handler if we can't connect to log service
	stdoutHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.MinLevel})

	// Create multi-handler that writes to both stdout and remote
	multiHandler := &multiHandler{
		handlers: []slog.Handler{stdoutHandler, handler},
	}

	logger := &Logger{
		Logger:   slog.New(multiHandler),
		entryCh:  entryCh,
		addr:     cfg.LogServiceAddr,
		source:   cfg.Source,
		minLevel: cfg.MinLevel,
	}

	// Start background sender if address is provided
	if cfg.LogServiceAddr != "" {
		ctx, cancel := context.WithCancel(context.Background())
		logger.cancel = cancel

		// Try initial connection
		logger.connect()

		// Start background sender
		go logger.runSender(ctx)
	}

	return logger
}

// Close shuts down the logger and releases resources.
func (l *Logger) Close() {
	if l.cancel != nil {
		l.cancel()
	}
	close(l.entryCh)
	l.mu.Lock()
	if l.conn != nil {
		l.conn.Close()
	}
	l.mu.Unlock()
}

func (l *Logger) connect() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn != nil {
		l.conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, l.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}

	l.conn = conn
	l.client = pb.NewLogServiceClient(conn)
	return nil
}

func (l *Logger) runSender(ctx context.Context) {
	backoff := reconnectBackoff

	for {
		select {
		case entry, ok := <-l.entryCh:
			if !ok {
				return
			}
			if err := l.sendEntry(entry); err != nil {
				// Try to reconnect with backoff
				time.Sleep(backoff)
				if l.connect() != nil {
					// Increase backoff up to 1 minute
					if backoff < time.Minute {
						backoff *= 2
					}
				} else {
					backoff = reconnectBackoff
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (l *Logger) sendEntry(entry *pb.LogEntry) error {
	l.mu.Lock()
	client := l.client
	l.mu.Unlock()

	if client == nil {
		return nil // No connection, skip
	}

	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()

	_, err := client.PushLog(ctx, &pb.PushLogRequest{Entry: entry})
	return err
}

// multiHandler sends log records to multiple handlers.
type multiHandler struct {
	handlers []slog.Handler
}

func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range m.handlers {
		if h.Enabled(ctx, r.Level) {
			// Ignore errors - we want to send to all handlers
			h.Handle(ctx, r)
		}
	}
	return nil
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		handlers[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: handlers}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		handlers[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: handlers}
}
