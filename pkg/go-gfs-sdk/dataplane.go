package gfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	pb "eddisonso.com/go-gfs/gen/master"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"github.com/golang-jwt/jwt/v5"
)

// ReadFile reads the entire file into memory.
func (c *Client) ReadFile(ctx context.Context, path string) ([]byte, error) {
	return c.ReadFileWithNamespace(ctx, path, "")
}

// ReadFileWithNamespace reads the entire file into memory with a namespace.
func (c *Client) ReadFileWithNamespace(ctx context.Context, path, namespace string) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := c.ReadToWithNamespace(ctx, path, namespace, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ReadTo streams file contents to the provided writer.
// Chunks are read sequentially and streamed directly without buffering.
func (c *Client) ReadTo(ctx context.Context, path string, w io.Writer) (int64, error) {
	return c.ReadToWithNamespace(ctx, path, "", w)
}

// ReadToWithNamespace streams file contents to the provided writer with a namespace.
// Uses parallel chunk reads with in-order delivery for improved throughput.
func (c *Client) ReadToWithNamespace(ctx context.Context, path, namespace string, w io.Writer) (int64, error) {
	chunks, err := c.GetChunkLocationsWithNamespace(ctx, path, namespace)
	if err != nil {
		return 0, err
	}
	if len(chunks) == 0 {
		return 0, ErrNoChunkLocations
	}

	// For single chunk, read directly without parallelism overhead
	if len(chunks) == 1 {
		chunkCtx, cancel := context.WithTimeout(ctx, c.chunkTimeout)
		defer cancel()
		return c.readChunkWithFailover(chunkCtx, chunks[0], w)
	}

	return c.readChunksParallel(ctx, chunks, w)
}

// chunkResult holds the result of reading a single chunk.
type chunkResult struct {
	index int
	data  []byte
	err   error
}

// readChunksParallel reads chunks concurrently and writes them in order.
func (c *Client) readChunksParallel(ctx context.Context, chunks []*pb.ChunkLocationInfo, w io.Writer) (int64, error) {
	numChunks := len(chunks)
	concurrency := c.readConcurrency
	if concurrency > numChunks {
		concurrency = numChunks
	}

	// Channel for work items (chunk indices)
	work := make(chan int, numChunks)
	// Channel for results
	results := make(chan chunkResult, concurrency)
	// Context for cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start worker goroutines
	for i := 0; i < concurrency; i++ {
		go func() {
			for idx := range work {
				var buf bytes.Buffer
				chunkCtx, chunkCancel := context.WithTimeout(ctx, c.chunkTimeout)
				_, err := c.readChunkWithFailover(chunkCtx, chunks[idx], &buf)
				chunkCancel()

				select {
				case results <- chunkResult{index: idx, data: buf.Bytes(), err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Send work items
	go func() {
		for i := 0; i < numChunks; i++ {
			select {
			case work <- i:
			case <-ctx.Done():
				close(work)
				return
			}
		}
		close(work)
	}()

	// Collect results and write in order
	var total int64
	pending := make(map[int][]byte) // Buffer for out-of-order chunks
	nextExpected := 0
	received := 0

	for received < numChunks {
		select {
		case result := <-results:
			received++
			if result.err != nil {
				cancel()
				return total, fmt.Errorf("failed to read chunk %d: %w", result.index, result.err)
			}

			if result.index == nextExpected {
				// Write this chunk and any buffered subsequent chunks
				n, err := w.Write(result.data)
				total += int64(n)
				if err != nil {
					cancel()
					return total, err
				}
				nextExpected++

				// Write any buffered chunks that are now in order
				for {
					if data, ok := pending[nextExpected]; ok {
						n, err := w.Write(data)
						total += int64(n)
						if err != nil {
							cancel()
							return total, err
						}
						delete(pending, nextExpected)
						nextExpected++
					} else {
						break
					}
				}
			} else {
				// Buffer out-of-order chunk
				pending[result.index] = result.data
			}

		case <-ctx.Done():
			return total, ctx.Err()
		}
	}

	return total, nil
}

// Append adds data to the end of the file, splitting across chunks as needed.
func (c *Client) Append(ctx context.Context, path string, data []byte) (int, error) {
	return c.AppendWithNamespace(ctx, path, "", data)
}

// AppendFrom streams data from a reader and appends it to the file.
func (c *Client) AppendFrom(ctx context.Context, path string, r io.Reader) (int64, error) {
	return c.AppendFromWithNamespace(ctx, path, "", r)
}

// AppendWithNamespace adds data to the end of the file in a namespace.
func (c *Client) AppendWithNamespace(ctx context.Context, path, namespace string, data []byte) (int, error) {
	return c.appendData(ctx, path, normalizeNamespace(namespace), data)
}

// AppendFromWithNamespace streams data from a reader and appends it to the file in a namespace.
// Uses double-buffering to overlap reading with writing for better throughput.
// Also uses async chunk pre-allocation to minimize pauses between chunks.
func (c *Client) AppendFromWithNamespace(ctx context.Context, path, namespace string, r io.Reader) (int64, error) {
	// Use PreparedUpload for chunk pre-fetching capability
	prep, err := c.PrepareUploadWithNamespace(ctx, path, namespace, 0)
	if err != nil {
		return 0, err
	}
	return prep.AppendFrom(ctx, r)
}

// writeResult holds the result of an async write operation.
type writeResult struct {
	written int
	err     error
}

// AppendFrom streams data using pre-allocated chunks.
// This is faster than Client.AppendFrom because chunks are already allocated.
// Uses triple-buffering with 2 in-flight writes to overlap commit latency.
func (p *PreparedUpload) AppendFrom(ctx context.Context, r io.Reader) (int64, error) {
	c := p.client

	// Triple buffer for pipelining: allows 2 writes in-flight while reading next
	// This overlaps the commit wait of chunk N with the data transfer of chunk N+1
	const numBuffers = 3
	const maxInFlight = 2
	bufs := make([][]byte, numBuffers)
	for i := range bufs {
		bufs[i] = make([]byte, int(c.maxChunkSize))
	}

	var total int64
	bufIdx := 0

	// Track in-flight writes with their results
	type inflightWrite struct {
		resultChan chan writeResult
		bufIdx     int
	}
	inflight := make([]inflightWrite, 0, maxInFlight)

	// Helper to wait for oldest write if we have too many in-flight
	waitForOldest := func() error {
		if len(inflight) == 0 {
			return nil
		}
		result := <-inflight[0].resultChan
		total += int64(result.written)
		inflight = inflight[1:]
		return result.err
	}

	for {
		// Read into current buffer
		n, readErr := io.ReadFull(r, bufs[bufIdx])

		// If we have max in-flight writes, wait for the oldest to complete
		if len(inflight) >= maxInFlight {
			if err := waitForOldest(); err != nil {
				// Drain remaining in-flight writes before returning
				for _, iw := range inflight {
					<-iw.resultChan
				}
				return total, err
			}
		}

		if n > 0 {
			// Start async write
			data := bufs[bufIdx][:n]
			resultChan := make(chan writeResult, 1)

			go func(data []byte, ch chan writeResult) {
				written, err := p.appendData(ctx, data)
				ch <- writeResult{written: written, err: err}
			}(data, resultChan)

			inflight = append(inflight, inflightWrite{resultChan: resultChan, bufIdx: bufIdx})

			// Move to next buffer
			bufIdx = (bufIdx + 1) % numBuffers
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			// Drain in-flight writes
			for _, iw := range inflight {
				result := <-iw.resultChan
				total += int64(result.written)
			}
			return total, readErr
		}
	}

	// Wait for all remaining in-flight writes
	var firstErr error
	for _, iw := range inflight {
		result := <-iw.resultChan
		total += int64(result.written)
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
	}

	return total, firstErr
}

// startPrefetch starts allocating the next chunk in the background.
// Note: If the upload fails before consuming the pre-fetched chunk, that chunk
// becomes orphaned in the master's metadata. It will be cleaned up when the file
// is deleted, or by future garbage collection. This trade-off is acceptable for
// the significant performance improvement from reduced inter-chunk latency.
func (p *PreparedUpload) startPrefetch(ctx context.Context) {
	if p.prefetching {
		return
	}
	p.prefetching = true
	p.prefetchCh = make(chan *pb.ChunkLocationInfo, 1)
	go func() {
		// Use a separate context so prefetch isn't cancelled if parent context times out
		// during chunk write (we still want the prefetch to complete)
		prefetchCtx, cancel := context.WithTimeout(context.Background(), p.client.chunkTimeout)
		defer cancel()
		chunk, err := p.client.AllocateChunkWithNamespace(prefetchCtx, p.path, p.namespace)
		if err != nil {
			p.prefetchErr = err
			close(p.prefetchCh)
			return
		}
		p.prefetchCh <- chunk
		close(p.prefetchCh)
	}()
}

// getPrefetchedChunk retrieves the pre-fetched chunk, waiting if needed.
func (p *PreparedUpload) getPrefetchedChunk() (*pb.ChunkLocationInfo, error) {
	if !p.prefetching {
		return nil, nil
	}
	chunk, ok := <-p.prefetchCh
	p.prefetching = false
	if !ok {
		return nil, p.prefetchErr
	}
	return chunk, nil
}

// appendData writes data, allocating new chunks on-demand as needed.
// Thread-safe for concurrent calls (pipelined writes).
func (p *PreparedUpload) appendData(ctx context.Context, data []byte) (int, error) {
	c := p.client
	total := 0
	remaining := data

	for len(remaining) > 0 {
		// Lock while we claim a chunk and determine write parameters
		p.mu.Lock()

		// Allocate new chunk if needed
		if p.index >= len(p.chunks) {
			// Check for pre-fetched chunk first
			if prefetched, err := p.getPrefetchedChunk(); err != nil {
				p.mu.Unlock()
				return total, fmt.Errorf("failed to allocate chunk %d for %s: %w", p.index, p.path, err)
			} else if prefetched != nil {
				p.chunks = append(p.chunks, prefetched)
			} else {
				// No pre-fetch, allocate synchronously (still under lock)
				p.mu.Unlock()
				chunk, err := c.AllocateChunkWithNamespace(ctx, p.path, p.namespace)
				if err != nil {
					return total, fmt.Errorf("failed to allocate chunk %d for %s: %w", p.index, p.path, err)
				}
				p.mu.Lock()
				p.chunks = append(p.chunks, chunk)
			}
		}

		// Get current chunk and calculate space
		chunkIdx := p.index
		chunk := p.chunks[chunkIdx]
		spaceAvailable := c.maxChunkSize - int64(chunk.Size)

		if spaceAvailable <= 0 {
			// Chunk is full, move to next
			p.index++
			p.mu.Unlock()
			continue
		}

		writeSize := len(remaining)
		if int64(writeSize) > spaceAvailable {
			writeSize = int(spaceAvailable)
		}

		// Reserve this space by updating chunk size now
		chunk.Size += uint64(writeSize)

		// If this write will fill the chunk, move index and pre-fetch
		willFillChunk := int64(writeSize) >= spaceAvailable
		if willFillChunk {
			p.index++
			if p.index >= len(p.chunks) {
				p.startPrefetch(ctx)
			}
		}

		// Capture progress base before unlocking
		baseBytes := p.bytesWritten
		p.bytesWritten += int64(writeSize)

		p.mu.Unlock()

		// Now do the actual write without holding the lock
		chunkData := remaining[:writeSize]
		remaining = remaining[writeSize:]

		// Retry loop for transient ErrNoPrimary (when chunkservers haven't registered yet)
		const maxRetries = 5
		var primary csstructs.ReplicaIdentifier
		var replicas []csstructs.ReplicaIdentifier
		var err error
		for attempt := 0; attempt < maxRetries; attempt++ {
			primary, replicas, err = c.buildWriteTargets(chunk)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrNoPrimary) {
				return total, err
			}
			// Primary not assigned yet - wait and retry with backoff
			backoff := time.Duration(1<<attempt) * 100 * time.Millisecond
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return total, ctx.Err()
			}
			// Re-fetch chunk info from master
			chunkCtx, cancel := context.WithTimeout(ctx, c.chunkTimeout)
			refreshedChunks, refreshErr := c.GetChunkLocationsWithNamespace(chunkCtx, p.path, p.namespace)
			cancel()
			if refreshErr != nil {
				return total, refreshErr
			}
			if chunkIdx < len(refreshedChunks) {
				chunk = refreshedChunks[chunkIdx]
				p.mu.Lock()
				p.chunks[chunkIdx] = chunk
				p.mu.Unlock()
			}
		}
		if err != nil {
			return total, err
		}

		// Progress callback using baseBytes captured earlier
		var progressCallback writeChunkProgress
		if p.onProgress != nil {
			progressCallback = func(bytesSent int) {
				p.onProgress(baseBytes + int64(bytesSent))
			}
		}

		writeCtx, writeCancel := context.WithTimeout(ctx, c.chunkTimeout)
		_, err = c.writeChunkWithProgress(writeCtx, primary, replicas, chunk.ChunkHandle, chunkData, -1, progressCallback)
		writeCancel()
		if err != nil {
			return total, fmt.Errorf("failed to write %d bytes to chunk %s (index %d): %w",
				len(chunkData), chunk.ChunkHandle, chunkIdx, err)
		}

		// Size, index, and bytesWritten already updated before the write
		total += writeSize
	}

	return total, nil
}

func (c *Client) appendData(ctx context.Context, path, namespace string, data []byte) (int, error) {
	total := 0
	remaining := data

	for len(remaining) > 0 {
		chunkCtx, cancel := context.WithTimeout(ctx, c.chunkTimeout)
		chunk, spaceAvailable, err := c.getChunkForAppend(chunkCtx, path, namespace)
		cancel()
		if err != nil {
			// Auto-create file if it doesn't exist
			if strings.Contains(err.Error(), "file not found") && !c.isFileKnown(path, namespace) {
				if _, createErr := c.CreateFileWithNamespace(ctx, path, namespace); createErr != nil {
					// If file already exists (race condition), that's fine
					if !strings.Contains(createErr.Error(), "already exists") {
						return total, fmt.Errorf("failed to create file: %w", createErr)
					}
				}
				c.markFileKnown(path, namespace)
				// Retry getting chunk after creating file
				chunkCtx, cancel = context.WithTimeout(ctx, c.chunkTimeout)
				chunk, spaceAvailable, err = c.getChunkForAppend(chunkCtx, path, namespace)
				cancel()
				if err != nil {
					return total, err
				}
			} else {
				return total, err
			}
		} else {
			// File exists, mark as known for future appends
			c.markFileKnown(path, namespace)
		}
		if spaceAvailable <= 0 {
			return total, fmt.Errorf("no space available in chunk")
		}

		writeSize := len(remaining)
		if int64(writeSize) > spaceAvailable {
			writeSize = int(spaceAvailable)
		}

		chunkData := remaining[:writeSize]
		remaining = remaining[writeSize:]

		// Retry loop for transient ErrNoPrimary (when chunkservers haven't registered yet)
		const maxRetries = 5
		var primary csstructs.ReplicaIdentifier
		var replicas []csstructs.ReplicaIdentifier
		for attempt := 0; attempt < maxRetries; attempt++ {
			primary, replicas, err = c.buildWriteTargets(chunk)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrNoPrimary) {
				return total, err
			}
			// Primary not assigned yet - invalidate cache and retry with backoff
			c.invalidateChunkCache(path, namespace)
			backoff := time.Duration(1<<attempt) * 100 * time.Millisecond
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return total, ctx.Err()
			}
			// Re-fetch chunk info
			chunkCtx, cancel := context.WithTimeout(ctx, c.chunkTimeout)
			chunk, _, err = c.getChunkForAppend(chunkCtx, path, namespace)
			cancel()
			if err != nil {
				return total, err
			}
		}
		if err != nil {
			return total, err
		}

		writeCtx, writeCancel := context.WithTimeout(ctx, c.chunkTimeout)
		if _, err := c.writeChunk(writeCtx, primary, replicas, chunk.ChunkHandle, chunkData, -1); err != nil {
			writeCancel()
			return total, err
		}
		writeCancel()

		// Update cached chunk size after successful write
		c.updateCachedChunkSize(path, namespace, chunk.ChunkHandle, uint64(writeSize))

		total += writeSize
	}

	return total, nil
}

func (c *Client) writeAtData(ctx context.Context, path, namespace string, data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, ErrInvalidOffset
	}

	total := 0
	remaining := data
	currentOffset := offset

	for len(remaining) > 0 {
		chunkIndex := int(currentOffset / c.maxChunkSize)
		offsetInChunk := currentOffset % c.maxChunkSize

		chunkCtx, cancel := context.WithTimeout(ctx, c.chunkTimeout)
		chunk, err := c.getChunkByIndex(chunkCtx, path, namespace, chunkIndex)
		cancel()
		if err != nil {
			return total, err
		}

		writeSize := len(remaining)
		maxWrite := int(c.maxChunkSize - offsetInChunk)
		if writeSize > maxWrite {
			writeSize = maxWrite
		}

		chunkData := remaining[:writeSize]
		remaining = remaining[writeSize:]

		// Retry loop for transient ErrNoPrimary (when chunkservers haven't registered yet)
		const maxRetries = 5
		var primary csstructs.ReplicaIdentifier
		var replicas []csstructs.ReplicaIdentifier
		for attempt := 0; attempt < maxRetries; attempt++ {
			primary, replicas, err = c.buildWriteTargets(chunk)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrNoPrimary) {
				return total, err
			}
			// Primary not assigned yet - invalidate cache and retry with backoff
			c.invalidateChunkCache(path, namespace)
			backoff := time.Duration(1<<attempt) * 100 * time.Millisecond
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return total, ctx.Err()
			}
			// Re-fetch chunk info
			chunkCtx, cancel := context.WithTimeout(ctx, c.chunkTimeout)
			chunk, err = c.getChunkByIndex(chunkCtx, path, namespace, chunkIndex)
			cancel()
			if err != nil {
				return total, err
			}
		}
		if err != nil {
			return total, err
		}

		writeCtx, writeCancel := context.WithTimeout(ctx, c.chunkTimeout)
		if _, err := c.writeChunk(writeCtx, primary, replicas, chunk.ChunkHandle, chunkData, int64(offsetInChunk)); err != nil {
			writeCancel()
			return total, err
		}
		writeCancel()

		total += writeSize
		currentOffset += int64(writeSize)
	}

	return total, nil
}

func (c *Client) getChunkForAppend(ctx context.Context, path, namespace string) (*pb.ChunkLocationInfo, int64, error) {
	chunks, err := c.getCachedChunks(ctx, path, namespace)
	if err != nil {
		return nil, 0, err
	}

	if len(chunks) > 0 {
		last := chunks[len(chunks)-1]
		space := c.maxChunkSize - int64(last.Size)
		if space > 0 {
			return last, space, nil
		}
	}

	// Allocate new chunk and add to cache
	alloc, err := c.AllocateChunkWithNamespace(ctx, path, namespace)
	if err != nil {
		return nil, 0, err
	}
	c.appendCachedChunk(path, namespace, alloc)
	return alloc, c.maxChunkSize, nil
}

func (c *Client) getChunkByIndex(ctx context.Context, path, namespace string, index int) (*pb.ChunkLocationInfo, error) {
	if index < 0 {
		return nil, ErrInvalidOffset
	}

	chunks, err := c.GetChunkLocationsWithNamespace(ctx, path, namespace)
	if err != nil {
		return nil, err
	}

	if len(chunks) > index {
		return chunks[index], nil
	}

	var chunk *pb.ChunkLocationInfo
	for i := len(chunks); i <= index; i++ {
		chunk, err = c.AllocateChunkWithNamespace(ctx, path, namespace)
		if err != nil {
			return nil, err
		}
	}

	return chunk, nil
}

func (c *Client) buildWriteTargets(info *pb.ChunkLocationInfo) (csstructs.ReplicaIdentifier, []csstructs.ReplicaIdentifier, error) {
	if info.Primary == nil {
		return csstructs.ReplicaIdentifier{}, nil, ErrNoPrimary
	}

	// Validate that primary has valid hostname and port
	if info.Primary.Hostname == "" || info.Primary.DataPort == 0 {
		return csstructs.ReplicaIdentifier{}, nil, ErrNoPrimary
	}

	primary := csstructs.ReplicaIdentifier{
		ID:              info.Primary.ServerId,
		Hostname:        info.Primary.Hostname,
		DataPort:        int(info.Primary.DataPort),
		ReplicationPort: int(info.Primary.ReplicationPort),
	}

	replicas := make([]csstructs.ReplicaIdentifier, 0, len(info.Locations))
	for _, loc := range info.Locations {
		if loc.ServerId == info.Primary.ServerId {
			continue
		}
		replicas = append(replicas, csstructs.ReplicaIdentifier{
			ID:              loc.ServerId,
			Hostname:        loc.Hostname,
			DataPort:        int(loc.DataPort),
			ReplicationPort: int(loc.ReplicationPort),
		})
	}

	return primary, replicas, nil
}

// writeChunkProgress is called during data transmission with bytes sent so far within this chunk.
type writeChunkProgress func(bytesSent int)

func (c *Client) writeChunk(ctx context.Context, primary csstructs.ReplicaIdentifier, replicas []csstructs.ReplicaIdentifier, chunkHandle string, data []byte, offset int64) (uint64, error) {
	return c.writeChunkWithProgress(ctx, primary, replicas, chunkHandle, data, offset, nil)
}

func (c *Client) writeChunkWithProgress(ctx context.Context, primary csstructs.ReplicaIdentifier, replicas []csstructs.ReplicaIdentifier, chunkHandle string, data []byte, offset int64, onProgress writeChunkProgress) (uint64, error) {
	conn, err := dialWithContext(ctx, primary.Hostname, primary.DataPort)
	if err != nil {
		return 0, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	if err := setDeadlineFromContext(ctx, conn); err != nil {
		return 0, err
	}

	actionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(actionBytes, uint32(csstructs.Download))
	if _, err = conn.Write(actionBytes); err != nil {
		return 0, fmt.Errorf("failed to send action: %w", err)
	}

	claims := csstructs.DownloadRequestClaims{
		ChunkHandle: chunkHandle,
		Operation:   "download",
		Filesize:    uint64(len(data)),
		Offset:      offset,
		Replicas:    replicas,
		Primary:     primary,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	secret, err := c.secretProvider(nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get secret: %w", err)
	}

	tokenString, err := token.SignedString(secret)
	if err != nil {
		return 0, fmt.Errorf("failed to sign token: %w", err)
	}

	tokenLen := uint32(len(tokenString))
	if err = binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
		return 0, fmt.Errorf("failed to send token length: %w", err)
	}

	if _, err = conn.Write([]byte(tokenString)); err != nil {
		return 0, fmt.Errorf("failed to send token: %w", err)
	}

	offsetBytes := make([]byte, 8)
	if _, err = io.ReadFull(conn, offsetBytes); err != nil {
		return 0, fmt.Errorf("failed to receive offset: %w", err)
	}
	offsetValue := binary.BigEndian.Uint64(offsetBytes)

	// Write data in smaller chunks to enable progress reporting
	const writeChunkSize = 1 << 20 // 1MB increments for progress updates
	totalSent := 0
	remaining := data
	for len(remaining) > 0 {
		writeSize := writeChunkSize
		if writeSize > len(remaining) {
			writeSize = len(remaining)
		}
		n, err := conn.Write(remaining[:writeSize])
		if err != nil {
			return 0, fmt.Errorf("failed to send data: %w", err)
		}
		totalSent += n
		remaining = remaining[n:]
		if onProgress != nil {
			onProgress(totalSent)
		}
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
	}

	resultBytes := make([]byte, 1)
	if _, err = io.ReadFull(conn, resultBytes); err != nil {
		return 0, fmt.Errorf("failed to receive commit response: %w", err)
	}

	if resultBytes[0] != 1 {
		return 0, fmt.Errorf("data persistence failed")
	}

	return offsetValue, nil
}

func (c *Client) readChunk(ctx context.Context, server csstructs.ReplicaIdentifier, chunkHandle string, w io.Writer) (int64, error) {
	conn, err := dialWithContext(ctx, server.Hostname, server.DataPort)
	if err != nil {
		return 0, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	if err := setDeadlineFromContext(ctx, conn); err != nil {
		return 0, err
	}

	actionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(actionBytes, uint32(csstructs.Upload))
	if _, err = conn.Write(actionBytes); err != nil {
		return 0, fmt.Errorf("failed to send action: %w", err)
	}

	claims := csstructs.UploadRequestClaims{
		ChunkHandle: chunkHandle,
		Operation:   "upload",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	secret, err := c.secretProvider(nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get secret: %w", err)
	}

	tokenString, err := token.SignedString(secret)
	if err != nil {
		return 0, fmt.Errorf("failed to sign token: %w", err)
	}

	tokenLen := uint32(len(tokenString))
	if err = binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
		return 0, fmt.Errorf("failed to send token length: %w", err)
	}
	if _, err = conn.Write([]byte(tokenString)); err != nil {
		return 0, fmt.Errorf("failed to send token: %w", err)
	}

	statusBytes := make([]byte, 1)
	if _, err = io.ReadFull(conn, statusBytes); err != nil {
		return 0, fmt.Errorf("failed to read status: %w", err)
	}

	if statusBytes[0] == 0 {
		codeBytes := make([]byte, 4)
		if _, err = io.ReadFull(conn, codeBytes); err != nil {
			return 0, fmt.Errorf("failed to read error code: %w", err)
		}
		errorCode := binary.BigEndian.Uint32(codeBytes)

		lenBytes := make([]byte, 4)
		if _, err = io.ReadFull(conn, lenBytes); err != nil {
			return 0, fmt.Errorf("failed to read error length: %w", err)
		}
		msgLen := binary.BigEndian.Uint32(lenBytes)

		msgBytes := make([]byte, msgLen)
		if _, err = io.ReadFull(conn, msgBytes); err != nil {
			return 0, fmt.Errorf("failed to read error message: %w", err)
		}

		return 0, fmt.Errorf("read failed: code=%d, message=%s", errorCode, string(msgBytes))
	}

	sizeBytes := make([]byte, 8)
	if _, err = io.ReadFull(conn, sizeBytes); err != nil {
		return 0, fmt.Errorf("failed to read file size: %w", err)
	}
	fileSize := binary.BigEndian.Uint64(sizeBytes)

	written, err := io.CopyN(w, conn, int64(fileSize))
	if err != nil && err != io.EOF {
		return written, fmt.Errorf("failed to stream data: %w", err)
	}

	return written, nil
}

func dialWithContext(ctx context.Context, host string, port int) (net.Conn, error) {
	dialer := net.Dialer{}
	return dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", host, port))
}

// toReplicaIdentifier converts a ChunkServerInfo to a ReplicaIdentifier.
func toReplicaIdentifier(s *pb.ChunkServerInfo) csstructs.ReplicaIdentifier {
	return csstructs.ReplicaIdentifier{
		ID:              s.ServerId,
		Hostname:        s.Hostname,
		DataPort:        int(s.DataPort),
		ReplicationPort: int(s.ReplicationPort),
	}
}

// buildReadTargets returns an ordered list of replicas to try for reading a chunk.
// The preferred replica (from replicaPicker) is first, followed by other replicas.
func (c *Client) buildReadTargets(chunk *pb.ChunkLocationInfo) []csstructs.ReplicaIdentifier {
	preferred := c.replicaPicker(chunk)

	var targets []csstructs.ReplicaIdentifier
	seen := make(map[string]bool)

	// Add preferred first
	if preferred != nil {
		targets = append(targets, toReplicaIdentifier(preferred))
		seen[preferred.ServerId] = true
	}

	// Add remaining replicas from locations
	for _, loc := range chunk.Locations {
		if seen[loc.ServerId] {
			continue
		}
		targets = append(targets, toReplicaIdentifier(loc))
		seen[loc.ServerId] = true
	}

	// Add primary if not already included
	if chunk.Primary != nil && !seen[chunk.Primary.ServerId] {
		targets = append(targets, toReplicaIdentifier(chunk.Primary))
	}

	return targets
}

// readChunkWithFailover tries to read a chunk from each available replica until one succeeds.
func (c *Client) readChunkWithFailover(ctx context.Context, chunk *pb.ChunkLocationInfo, w io.Writer) (int64, error) {
	replicas := c.buildReadTargets(chunk)
	if len(replicas) == 0 {
		return 0, fmt.Errorf("no replicas available for chunk %s", chunk.ChunkHandle)
	}

	var lastErr error
	for _, replica := range replicas {
		n, err := c.readChunk(ctx, replica, chunk.ChunkHandle, w)
		if err == nil {
			return n, nil
		}
		lastErr = err
	}

	return 0, fmt.Errorf("all %d replicas failed for chunk %s: %w", len(replicas), chunk.ChunkHandle, lastErr)
}

func setDeadlineFromContext(ctx context.Context, conn net.Conn) error {
	if deadline, ok := ctx.Deadline(); ok {
		return conn.SetDeadline(deadline)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// getCachedChunks returns cached chunk locations or fetches from master if not cached.
func (c *Client) getCachedChunks(ctx context.Context, path, namespace string) ([]*pb.ChunkLocationInfo, error) {
	key := fileKey{namespace: namespace, path: path}

	// Check cache first
	c.chunkCacheMu.RLock()
	cache, exists := c.chunkCache[key]
	c.chunkCacheMu.RUnlock()

	if exists {
		cache.mu.RLock()
		chunks := cache.chunks
		cache.mu.RUnlock()
		return chunks, nil
	}

	// Fetch from master
	chunks, err := c.GetChunkLocationsWithNamespace(ctx, path, namespace)
	if err != nil {
		return nil, err
	}

	// Store in cache
	c.chunkCacheMu.Lock()
	c.chunkCache[key] = &chunkCache{chunks: chunks}
	c.chunkCacheMu.Unlock()

	return chunks, nil
}

// updateCachedChunkSize updates the size of a cached chunk after a successful write.
func (c *Client) updateCachedChunkSize(path, namespace, chunkHandle string, additionalBytes uint64) {
	key := fileKey{namespace: namespace, path: path}

	c.chunkCacheMu.RLock()
	cache, exists := c.chunkCache[key]
	c.chunkCacheMu.RUnlock()

	if !exists {
		return
	}

	cache.mu.Lock()
	for _, chunk := range cache.chunks {
		if chunk.ChunkHandle == chunkHandle {
			chunk.Size += additionalBytes
			break
		}
	}
	cache.mu.Unlock()
}

// appendCachedChunk adds a newly allocated chunk to the cache.
func (c *Client) appendCachedChunk(path, namespace string, chunk *pb.ChunkLocationInfo) {
	key := fileKey{namespace: namespace, path: path}

	c.chunkCacheMu.Lock()
	cache, exists := c.chunkCache[key]
	if !exists {
		cache = &chunkCache{}
		c.chunkCache[key] = cache
	}
	c.chunkCacheMu.Unlock()

	cache.mu.Lock()
	cache.chunks = append(cache.chunks, chunk)
	cache.mu.Unlock()
}

// invalidateChunkCache removes cached chunks for a file.
func (c *Client) invalidateChunkCache(path, namespace string) {
	key := fileKey{namespace: namespace, path: path}

	c.chunkCacheMu.Lock()
	delete(c.chunkCache, key)
	c.chunkCacheMu.Unlock()
}

// invalidateNamespaceCache removes all cached chunks for a namespace.
func (c *Client) invalidateNamespaceCache(namespace string) {
	c.chunkCacheMu.Lock()
	for key := range c.chunkCache {
		if key.namespace == namespace {
			delete(c.chunkCache, key)
		}
	}
	c.chunkCacheMu.Unlock()
}
