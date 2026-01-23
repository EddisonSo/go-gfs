package gfs

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// ConnPool manages a pool of TCP connections to chunkservers.
// Connections are pooled per host:port and reused across requests.
type ConnPool struct {
	mu          sync.Mutex
	pools       map[string]*hostPool // key: "host:port"
	maxIdlePerHost int
	idleTimeout    time.Duration
	dialer         net.Dialer
}

// hostPool holds idle connections for a single host:port.
type hostPool struct {
	conns []*pooledConn
}

// pooledConn wraps a net.Conn with metadata for pooling.
type pooledConn struct {
	net.Conn
	pool       *ConnPool
	addr       string
	idleSince  time.Time
	halfClosed bool // Set when CloseWrite() is called
}

// CloseWrite signals the end of writing but keeps the connection open for reading.
// After calling CloseWrite, the connection cannot be reused.
func (pc *pooledConn) CloseWrite() error {
	pc.halfClosed = true
	if tcpConn, ok := pc.Conn.(*net.TCPConn); ok {
		return tcpConn.CloseWrite()
	}
	return nil
}

// IsHalfClosed returns true if CloseWrite has been called.
func (pc *pooledConn) IsHalfClosed() bool {
	return pc.halfClosed
}

// NewConnPool creates a new connection pool.
func NewConnPool(maxIdlePerHost int, idleTimeout time.Duration) *ConnPool {
	if maxIdlePerHost <= 0 {
		maxIdlePerHost = 4
	}
	if idleTimeout <= 0 {
		idleTimeout = 30 * time.Second
	}
	p := &ConnPool{
		pools:          make(map[string]*hostPool),
		maxIdlePerHost: maxIdlePerHost,
		idleTimeout:    idleTimeout,
	}
	// Start background cleanup goroutine
	go p.cleanupLoop()
	return p
}

// Get retrieves a connection from the pool or creates a new one.
func (p *ConnPool) Get(ctx context.Context, host string, port int) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)

	// Try to get an idle connection
	if conn := p.getIdle(addr); conn != nil {
		// Validate the connection is still alive
		if err := conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond)); err == nil {
			buf := make([]byte, 1)
			conn.SetReadDeadline(time.Time{}) // Clear deadline
			// Try a non-blocking read - if we get data or EOF, connection is broken
			conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
			_, err := conn.Read(buf)
			conn.SetReadDeadline(time.Time{})
			if err == nil {
				// Got unexpected data - connection is in bad state
				conn.Close()
			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Timeout is expected for a healthy idle connection
				return &pooledConn{Conn: conn, pool: p, addr: addr}, nil
			} else {
				// Connection is dead
				conn.Close()
			}
		}
	}

	// Create new connection
	conn, err := p.dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	return &pooledConn{Conn: conn, pool: p, addr: addr}, nil
}

// getIdle retrieves an idle connection for the given address.
func (p *ConnPool) getIdle(addr string) net.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	hp := p.pools[addr]
	if hp == nil || len(hp.conns) == 0 {
		return nil
	}

	// Get the most recently used connection (LIFO for better cache locality)
	idx := len(hp.conns) - 1
	pc := hp.conns[idx]
	hp.conns = hp.conns[:idx]

	// Check if connection has been idle too long
	if time.Since(pc.idleSince) > p.idleTimeout {
		pc.Conn.Close()
		return nil
	}

	return pc.Conn
}

// Put returns a connection to the pool for reuse.
// The connection should not be used after calling Put.
func (p *ConnPool) Put(conn net.Conn) {
	pc, ok := conn.(*pooledConn)
	if !ok {
		// Not a pooled connection, just close it
		conn.Close()
		return
	}

	// Don't pool half-closed connections (after CloseWrite)
	if pc.halfClosed {
		pc.Conn.Close()
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	hp := p.pools[pc.addr]
	if hp == nil {
		hp = &hostPool{}
		p.pools[pc.addr] = hp
	}

	// Check if we have room in the pool
	if len(hp.conns) >= p.maxIdlePerHost {
		// Pool is full, close the connection
		pc.Conn.Close()
		return
	}

	pc.idleSince = time.Now()
	hp.conns = append(hp.conns, pc)
}

// Close closes all idle connections and stops the cleanup goroutine.
func (p *ConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, hp := range p.pools {
		for _, pc := range hp.conns {
			pc.Conn.Close()
		}
	}
	p.pools = make(map[string]*hostPool)
}

// cleanupLoop periodically removes stale connections.
func (p *ConnPool) cleanupLoop() {
	ticker := time.NewTicker(p.idleTimeout / 2)
	defer ticker.Stop()

	for range ticker.C {
		p.cleanup()
	}
}

// cleanup removes connections that have been idle too long.
func (p *ConnPool) cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	for addr, hp := range p.pools {
		var keep []*pooledConn
		for _, pc := range hp.conns {
			if now.Sub(pc.idleSince) > p.idleTimeout {
				pc.Conn.Close()
			} else {
				keep = append(keep, pc)
			}
		}
		if len(keep) == 0 {
			delete(p.pools, addr)
		} else {
			hp.conns = keep
		}
	}
}

// Stats returns pool statistics.
func (p *ConnPool) Stats() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats := make(map[string]int)
	for addr, hp := range p.pools {
		stats[addr] = len(hp.conns)
	}
	return stats
}
