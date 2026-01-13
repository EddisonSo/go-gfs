package gfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

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

// chunkReadResult holds the result of reading a single chunk.
type chunkReadResult struct {
	index int
	data  []byte
	err   error
}

// ReadTo streams file contents to the provided writer.
// Chunks are read in parallel for better performance.
func (c *Client) ReadTo(ctx context.Context, path string, w io.Writer) (int64, error) {
	return c.ReadToWithNamespace(ctx, path, "", w)
}

// ReadToWithNamespace streams file contents to the provided writer with a namespace.
// Chunks are read in parallel with automatic failover to replica servers.
func (c *Client) ReadToWithNamespace(ctx context.Context, path, namespace string, w io.Writer) (int64, error) {
	chunks, err := c.GetChunkLocationsWithNamespace(ctx, path, namespace)
	if err != nil {
		return 0, err
	}
	if len(chunks) == 0 {
		return 0, ErrNoChunkLocations
	}

	// Read chunks in parallel with bounded concurrency and failover
	sem := make(chan struct{}, c.readConcurrency)
	results := make(chan chunkReadResult, len(chunks))
	var wg sync.WaitGroup

	for i, chunk := range chunks {
		wg.Add(1)
		go func(index int, chunk *pb.ChunkLocationInfo) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results <- chunkReadResult{index: index, err: ctx.Err()}
				return
			}

			var buf bytes.Buffer
			chunkCtx, cancel := context.WithTimeout(ctx, c.chunkTimeout)
			_, err := c.readChunkWithFailover(chunkCtx, chunk, &buf)
			cancel()

			if err != nil {
				results <- chunkReadResult{index: index, err: err}
				return
			}

			results <- chunkReadResult{index: index, data: buf.Bytes()}
		}(i, chunk)
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Stream results in order as they become available
	pending := make(map[int][]byte, len(chunks))
	nextIndex := 0
	var total int64
	var readErr error

	for result := range results {
		if result.err != nil {
			readErr = result.err
			continue
		}

		if result.index == nextIndex {
			n, err := w.Write(result.data)
			if err != nil {
				return total, err
			}
			total += int64(n)
			nextIndex++

			for {
				data, ok := pending[nextIndex]
				if !ok {
					break
				}
				delete(pending, nextIndex)
				n, err := w.Write(data)
				if err != nil {
					return total, err
				}
				total += int64(n)
				nextIndex++
			}
		} else {
			pending[result.index] = result.data
		}
	}

	if readErr != nil {
		return total, readErr
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
func (c *Client) AppendFromWithNamespace(ctx context.Context, path, namespace string, r io.Reader) (int64, error) {
	buf := make([]byte, int(c.maxChunkSize))
	var total int64

	for {
		// Use io.ReadFull to fill the buffer before writing.
		// This avoids making a gRPC call for every small HTTP chunk.
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			written, writeErr := c.appendData(ctx, path, normalizeNamespace(namespace), buf[:n])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// writeResult holds the result of an async write operation.
type writeResult struct {
	written int
	err     error
}

// AppendFrom streams data using pre-allocated chunks.
// This is faster than Client.AppendFrom because chunks are already allocated.
// Uses double-buffering to overlap reading with writing.
func (p *PreparedUpload) AppendFrom(ctx context.Context, r io.Reader) (int64, error) {
	c := p.client

	// Double buffer for pipelining: read next chunk while writing current
	bufs := [2][]byte{
		make([]byte, int(c.maxChunkSize)),
		make([]byte, int(c.maxChunkSize)),
	}

	var total int64
	var resultChan chan writeResult
	bufIdx := 0

	for {
		// Read into current buffer
		n, readErr := io.ReadFull(r, bufs[bufIdx])

		// Wait for previous write to complete before starting new one
		if resultChan != nil {
			result := <-resultChan
			total += int64(result.written)
			if result.err != nil {
				return total, result.err
			}
		}

		if n > 0 {
			// Start async write
			data := bufs[bufIdx][:n]
			resultChan = make(chan writeResult, 1)

			go func(data []byte, ch chan writeResult) {
				written, err := p.appendData(ctx, data)
				ch <- writeResult{written: written, err: err}
			}(data, resultChan)

			// Swap buffers for next read
			bufIdx = 1 - bufIdx
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			// Wait for pending write
			if resultChan != nil {
				result := <-resultChan
				total += int64(result.written)
			}
			return total, readErr
		}
	}

	// Wait for final write
	if resultChan != nil {
		result := <-resultChan
		total += int64(result.written)
		if result.err != nil {
			return total, result.err
		}
	}

	return total, nil
}

// appendData writes data, allocating new chunks on-demand as needed.
func (p *PreparedUpload) appendData(ctx context.Context, data []byte) (int, error) {
	c := p.client
	total := 0
	remaining := data

	for len(remaining) > 0 {
		// Allocate new chunk if needed
		if p.index >= len(p.chunks) {
			chunk, err := c.AllocateChunkWithNamespace(ctx, p.path, p.namespace)
			if err != nil {
				return total, fmt.Errorf("failed to allocate chunk: %w", err)
			}
			p.chunks = append(p.chunks, chunk)
		}
		chunk := p.chunks[p.index]

		// Calculate space available in current chunk
		spaceAvailable := c.maxChunkSize - int64(chunk.Size)
		if spaceAvailable <= 0 {
			// Chunk is full, move to next
			p.index++
			continue
		}

		writeSize := len(remaining)
		if int64(writeSize) > spaceAvailable {
			writeSize = int(spaceAvailable)
		}

		chunkData := remaining[:writeSize]
		remaining = remaining[writeSize:]

		primary, replicas, err := c.buildWriteTargets(chunk)
		if err != nil {
			return total, err
		}

		writeCtx, writeCancel := context.WithTimeout(ctx, c.chunkTimeout)
		_, err = c.writeChunk(writeCtx, primary, replicas, chunk.ChunkHandle, chunkData, -1)
		writeCancel()
		if err != nil {
			return total, err
		}

		// Update chunk size in our local copy
		chunk.Size += uint64(writeSize)
		total += writeSize

		// Track total bytes written and notify progress
		p.bytesWritten += int64(writeSize)
		if p.onProgress != nil {
			p.onProgress(p.bytesWritten)
		}

		// If chunk is full, move to next
		if int64(chunk.Size) >= c.maxChunkSize {
			p.index++
		}
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
			return total, err
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

		primary, replicas, err := c.buildWriteTargets(chunk)
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

		primary, replicas, err := c.buildWriteTargets(chunk)
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

func (c *Client) writeChunk(ctx context.Context, primary csstructs.ReplicaIdentifier, replicas []csstructs.ReplicaIdentifier, chunkHandle string, data []byte, offset int64) (uint64, error) {
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

	if _, err = conn.Write(data); err != nil {
		return 0, fmt.Errorf("failed to send data: %w", err)
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
