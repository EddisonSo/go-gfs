package gfslog

import (
	"context"
	"log/slog"
	"time"

	pb "eddisonso.com/go-gfs/gen/logging"
)

// Handler is a slog.Handler that sends logs to both stdout and a remote log service.
type Handler struct {
	stdout   slog.Handler
	source   string
	entryCh  chan *pb.LogEntry
	minLevel slog.Level
	attrs    []slog.Attr
	groups   []string
}

// NewHandler creates a new Handler that sends logs to both stdout and the log service.
func NewHandler(source string, minLevel slog.Level, entryCh chan *pb.LogEntry) *Handler {
	return &Handler{
		stdout:   slog.NewTextHandler(nil, &slog.HandlerOptions{Level: minLevel}),
		source:   source,
		entryCh:  entryCh,
		minLevel: minLevel,
	}
}

// Enabled reports whether the handler handles records at the given level.
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.minLevel
}

// Handle handles the Record.
func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	// Always write to stdout
	if err := h.stdout.Handle(ctx, r); err != nil {
		// Ignore stdout errors
	}

	// Convert to LogEntry and send to channel (non-blocking)
	entry := h.recordToEntry(r)
	select {
	case h.entryCh <- entry:
	default:
		// Drop if buffer full - don't block
	}

	return nil
}

// WithAttrs returns a new Handler with the given attributes.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newH := *h
	newH.attrs = append(newH.attrs, attrs...)
	newH.stdout = h.stdout.WithAttrs(attrs)
	return &newH
}

// WithGroup returns a new Handler with the given group name.
func (h *Handler) WithGroup(name string) slog.Handler {
	newH := *h
	newH.groups = append(newH.groups, name)
	newH.stdout = h.stdout.WithGroup(name)
	return &newH
}

func (h *Handler) recordToEntry(r slog.Record) *pb.LogEntry {
	entry := &pb.LogEntry{
		Source:     h.source,
		Level:      slogLevelToProto(r.Level),
		Message:    r.Message,
		Timestamp:  r.Time.Unix(),
		Attributes: make(map[string]string),
	}

	// Add pre-defined attributes
	for _, attr := range h.attrs {
		entry.Attributes[attr.Key] = attr.Value.String()
	}

	// Add record attributes
	r.Attrs(func(a slog.Attr) bool {
		key := a.Key
		for _, g := range h.groups {
			key = g + "." + key
		}
		entry.Attributes[key] = a.Value.String()
		return true
	})

	return entry
}

func slogLevelToProto(level slog.Level) pb.LogLevel {
	switch {
	case level >= slog.LevelError:
		return pb.LogLevel_ERROR
	case level >= slog.LevelWarn:
		return pb.LogLevel_WARN
	case level >= slog.LevelInfo:
		return pb.LogLevel_INFO
	default:
		return pb.LogLevel_DEBUG
	}
}

// senderConfig holds configuration for the background sender.
type senderConfig struct {
	addr        string
	entryCh     <-chan *pb.LogEntry
	reconnectFn func() error
}

// startSender starts the background goroutine that sends logs to the log service.
func startSender(ctx context.Context, cfg senderConfig, sendFn func(*pb.LogEntry) error) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case entry, ok := <-cfg.entryCh:
			if !ok {
				return
			}
			if err := sendFn(entry); err != nil {
				// Try to reconnect
				if cfg.reconnectFn != nil {
					cfg.reconnectFn()
				}
			}
		case <-ticker.C:
			// Periodic reconnect check could go here
		case <-ctx.Done():
			return
		}
	}
}
