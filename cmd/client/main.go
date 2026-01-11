package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "eddisonso.com/go-gfs/gen/master"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/term"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	masterAddr string
	client     pb.MasterClient
	conn       *grpc.ClientConn
)

// ============ Progress Bar ============

// TransferProgress tracks and displays transfer progress
type TransferProgress struct {
	total       int64
	current     int64
	startTime   time.Time
	lastUpdate  time.Time
	operation   string // "Uploading" or "Downloading"
	done        chan struct{}
	mu          sync.Mutex
	isTerminal  bool
	termWidth   int
}

// NewTransferProgress creates a new progress tracker
func NewTransferProgress(total int64, operation string) *TransferProgress {
	width := 40
	isTerminal := term.IsTerminal(int(os.Stdout.Fd()))
	if isTerminal {
		if w, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil && w > 0 {
			width = w
		}
	}
	return &TransferProgress{
		total:      total,
		operation:  operation,
		startTime:  time.Now(),
		lastUpdate: time.Now(),
		done:       make(chan struct{}),
		isTerminal: isTerminal,
		termWidth:  width,
	}
}

// Update sets the current progress
func (p *TransferProgress) Update(current int64) {
	p.mu.Lock()
	p.current = current
	p.mu.Unlock()
}

// Add increments the current progress
func (p *TransferProgress) Add(delta int64) {
	p.mu.Lock()
	p.current += delta
	p.mu.Unlock()
}

// Start begins rendering the progress bar
func (p *TransferProgress) Start() {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-p.done:
				p.render(true)
				return
			case <-ticker.C:
				p.render(false)
			}
		}
	}()
}

// Finish stops the progress bar and prints final state
func (p *TransferProgress) Finish() {
	close(p.done)
	time.Sleep(50 * time.Millisecond) // Allow final render
}

func (p *TransferProgress) render(final bool) {
	p.mu.Lock()
	current := p.current
	total := p.total
	p.mu.Unlock()

	elapsed := time.Since(p.startTime).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	percent := float64(0)
	if total > 0 {
		percent = float64(current) / float64(total) * 100
	}

	// Calculate speed
	speed := float64(current) / elapsed
	speedStr := formatBytes(int64(speed)) + "/s"

	// Calculate ETA
	eta := ""
	if speed > 0 && current < total {
		remaining := float64(total-current) / speed
		eta = formatDuration(time.Duration(remaining) * time.Second)
	}

	// Build progress bar
	barWidth := 30
	if p.termWidth < 80 {
		barWidth = 20
	}

	filled := int(percent / 100 * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}

	bar := strings.Repeat("=", filled)
	if filled < barWidth && !final {
		bar += ">"
		bar += strings.Repeat(" ", barWidth-filled-1)
	} else if filled < barWidth {
		bar += strings.Repeat(" ", barWidth-filled)
	}

	// Format sizes
	currentStr := formatBytes(current)
	totalStr := formatBytes(total)

	// Build output line
	line := fmt.Sprintf("\r%s [%s] %5.1f%% %s/%s %s",
		p.operation, bar, percent, currentStr, totalStr, speedStr)
	if eta != "" && !final {
		line += fmt.Sprintf(" ETA %s", eta)
	}

	// Pad with spaces to clear previous longer output
	if len(line) < p.termWidth {
		line += strings.Repeat(" ", p.termWidth-len(line))
	}

	if p.isTerminal {
		fmt.Print(line)
		if final {
			fmt.Println()
		}
	} else if final {
		// Non-terminal: just print final summary
		fmt.Printf("%s: %s (%s)\n", p.operation, totalStr, speedStr)
	}
}

// formatBytes formats bytes as human-readable string
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// formatDuration formats duration as human-readable string
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

// ProgressWriter wraps a writer and updates progress
type ProgressWriter struct {
	w        io.Writer
	progress *TransferProgress
}

func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	if n > 0 {
		pw.progress.Add(int64(n))
	}
	return n, err
}

// ProgressReader wraps a reader and updates progress
type ProgressReader struct {
	r        io.Reader
	progress *TransferProgress
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	if n > 0 {
		pr.progress.Add(int64(n))
	}
	return n, err
}

// AtomicCounter for thread-safe progress updates
type AtomicCounter struct {
	value int64
}

func (c *AtomicCounter) Add(delta int64) int64 {
	return atomic.AddInt64(&c.value, delta)
}

func (c *AtomicCounter) Load() int64 {
	return atomic.LoadInt64(&c.value)
}

func main() {
	masterAddr = "localhost:9000"

	// Check for -master flag
	for i, arg := range os.Args[1:] {
		if arg == "-master" && i+2 < len(os.Args) {
			masterAddr = os.Args[i+2]
		}
	}

	// Connect to master
	var err error
	conn, err = grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create connection: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client = pb.NewMasterClient(conn)

	// Verify connection by making a test RPC call
	fmt.Printf("Connecting to master at %s...\n", masterAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.ListFiles(ctx, &pb.ListFilesRequest{})
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to master: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Connected to %s\n", masterAddr)
	fmt.Println("Type 'help' for commands, 'exit' to quit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("gfs> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		args := parseArgs(line)
		if len(args) == 0 {
			continue
		}

		cmd := args[0]
		cmdArgs := args[1:]

		switch cmd {
		case "ls":
			cmdLs(cmdArgs)
		case "read", "cat":
			cmdRead(cmdArgs)
		case "write":
			cmdWrite(cmdArgs)
		case "mv", "rename":
			cmdMv(cmdArgs)
		case "rm":
			cmdRm(cmdArgs)
		case "info":
			cmdInfo(cmdArgs)
		case "pressure":
			cmdPressure(cmdArgs)
		case "help":
			printHelp()
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
			fmt.Println("Type 'help' for available commands")
		}
	}
}

func parseArgs(line string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, r := range line {
		if inQuote {
			if r == quoteChar {
				inQuote = false
			} else {
				current.WriteRune(r)
			}
		} else {
			if r == '"' || r == '\'' {
				inQuote = true
				quoteChar = r
			} else if r == ' ' || r == '\t' {
				if current.Len() > 0 {
					args = append(args, current.String())
					current.Reset()
				}
			} else {
				current.WriteRune(r)
			}
		}
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	return args
}

func printHelp() {
	fmt.Println(`Commands:
  ls                      List all files
  read <path>             Read file to stdout
  read <path> > <file>    Read file to local file
  write <path> <data>     Write data to file
  write <path> < <file>   Write local file to GFS
  mv <src> <dst>          Rename/move a file
  rm <path>               Delete a file
  info <path>             Show file information
  pressure                Show cluster resource pressure (CPU, memory, disk)
  help                    Show this help
  exit                    Quit the client`)
}

func getContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 120*time.Second)
}

// ============ ls command ============

func cmdLs(args []string) {
	ctx, cancel := getContext()
	defer cancel()

	prefix := ""
	if len(args) > 0 {
		prefix = args[0]
	}

	resp, err := client.ListFiles(ctx, &pb.ListFilesRequest{Prefix: prefix})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(resp.Files) == 0 {
		fmt.Println("No files found")
		return
	}

	for _, f := range resp.Files {
		fmt.Printf("%s\t%d chunks\t%d bytes\n", f.Path, len(f.ChunkHandles), f.Size)
	}
}

// ============ read command ============

// getServerLoads fetches cluster pressure and returns a map of server ID -> load score
func getServerLoads(ctx context.Context) map[string]float64 {
	loads := make(map[string]float64)

	resp, err := client.GetClusterPressure(ctx, &pb.GetClusterPressureRequest{})
	if err != nil {
		return loads
	}

	for _, server := range resp.Servers {
		if !server.IsAlive || server.Resources == nil {
			continue
		}
		// Combined score: CPU 40%, Memory 40%, Disk 20%
		r := server.Resources
		score := r.CpuUsagePercent*0.4 + r.MemoryUsagePercent*0.4 + r.DiskUsagePercent*0.2
		loads[server.Server.ServerId] = score
	}

	return loads
}

// selectReplicaByLoad selects a replica using weighted random selection
// Servers with lower load have higher probability of being selected
func selectReplicaByLoad(locations []*pb.ChunkServerInfo, loads map[string]float64) *pb.ChunkServerInfo {
	if len(locations) == 0 {
		return nil
	}
	if len(locations) == 1 {
		return locations[0]
	}

	// Calculate weights (inverse of load, so lower load = higher weight)
	type weightedServer struct {
		server *pb.ChunkServerInfo
		weight float64
	}

	servers := make([]weightedServer, 0, len(locations))
	var totalWeight float64

	for _, loc := range locations {
		load, ok := loads[loc.ServerId]
		if !ok {
			load = 50 // Default to 50% if no data
		}
		// Weight is inverse of load: (100 - load) gives us 0-100 where higher = less loaded
		// Add 1 to avoid zero weights
		weight := (100 - load) + 1
		servers = append(servers, weightedServer{server: loc, weight: weight})
		totalWeight += weight
	}

	// Weighted random selection
	r := rand.Float64() * totalWeight
	var cumulative float64
	for _, ws := range servers {
		cumulative += ws.weight
		if r <= cumulative {
			return ws.server
		}
	}

	// Fallback to last server
	return servers[len(servers)-1].server
}

// chunkReadResult holds the result of reading a single chunk
type chunkReadResult struct {
	index int
	data  []byte
	err   error
}

func cmdRead(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: read <path> [> localfile]")
		return
	}

	path := args[0]
	var outputFile string

	// Check for > redirect
	for i, arg := range args {
		if arg == ">" && i+1 < len(args) {
			outputFile = args[i+1]
			break
		}
	}

	ctx, cancel := getContext()
	defer cancel()

	// Get chunk locations
	locResp, err := client.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: path})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	if !locResp.Success {
		fmt.Printf("Error: %s\n", locResp.Message)
		return
	}

	if len(locResp.Chunks) == 0 {
		fmt.Println("No chunks found for file")
		return
	}

	// Calculate total file size for progress bar
	var totalSize int64
	for _, chunk := range locResp.Chunks {
		totalSize += int64(chunk.Size)
	}

	// Start progress bar for larger files (> 1MB) when writing to file
	var progress *TransferProgress
	var bytesRead AtomicCounter
	if totalSize > 1024*1024 && outputFile != "" {
		progress = NewTransferProgress(totalSize, "Downloading")
		progress.Start()
		defer progress.Finish()
	}

	// Get server load information for weighted replica selection
	loads := getServerLoads(ctx)

	// Read all chunks in parallel
	results := make(chan chunkReadResult, len(locResp.Chunks))
	var wg sync.WaitGroup

	for i, chunk := range locResp.Chunks {
		wg.Add(1)
		go func(index int, chunk *pb.ChunkLocationInfo) {
			defer wg.Done()

			// Select replica based on load (prefer less loaded servers)
			var server *pb.ChunkServerInfo
			if len(chunk.Locations) > 0 {
				server = selectReplicaByLoad(chunk.Locations, loads)
			} else if chunk.Primary != nil {
				server = chunk.Primary
			}

			if server == nil {
				results <- chunkReadResult{index: index, err: fmt.Errorf("no available servers for chunk %s", chunk.ChunkHandle)}
				return
			}

			replica := csstructs.ReplicaIdentifier{
				ID:              server.ServerId,
				Hostname:        server.Hostname,
				DataPort:        int(server.DataPort),
				ReplicationPort: int(server.ReplicationPort),
			}

			// Read into buffer
			var buf bytes.Buffer
			n, err := performReadStream(replica, chunk.ChunkHandle, &buf)
			if err != nil {
				results <- chunkReadResult{index: index, err: fmt.Errorf("failed to read chunk %s from %s: %w", chunk.ChunkHandle, server.ServerId, err)}
				return
			}

			// Update progress
			if progress != nil {
				progress.Update(bytesRead.Add(n))
			}

			results <- chunkReadResult{index: index, data: buf.Bytes()}
		}(i, chunk)
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	chunkData := make([][]byte, len(locResp.Chunks))
	var readErr error
	for result := range results {
		if result.err != nil {
			readErr = result.err
			continue
		}
		chunkData[result.index] = result.data
	}

	if readErr != nil {
		fmt.Printf("Error: %v\n", readErr)
		return
	}

	// Set up output writer
	var output io.Writer
	var outFile *os.File
	if outputFile != "" {
		outFile, err = os.Create(outputFile)
		if err != nil {
			fmt.Printf("Failed to create output file: %v\n", err)
			return
		}
		defer outFile.Close()
		output = outFile
	} else {
		output = os.Stdout
	}

	// Write chunks in order
	var totalBytes int64
	for _, data := range chunkData {
		n, err := output.Write(data)
		if err != nil {
			fmt.Printf("Failed to write output: %v\n", err)
			return
		}
		totalBytes += int64(n)
	}

	// Add newline to stdout if needed (for text files)
	if outputFile == "" {
		fmt.Println()
	} else if progress == nil {
		// Only print message if we didn't show a progress bar
		fmt.Printf("Wrote %d bytes to %s\n", totalBytes, outputFile)
	}
}

// ============ write command ============

const maxChunkSize = 64 << 20 // 64MB

func cmdWrite(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: write <path> <data>  OR  write <path> < localfile")
		return
	}

	path := args[0]

	// Check for < redirect
	inputFile := ""
	for i, arg := range args {
		if arg == "<" && i+1 < len(args) {
			inputFile = args[i+1]
			break
		}
	}

	ctx, cancel := getContext()
	defer cancel()

	// Create file if it doesn't exist
	client.CreateFile(ctx, &pb.CreateFileRequest{Path: path})

	if inputFile != "" {
		// Stream from file
		totalWritten, err := writeFromFile(ctx, path, inputFile)
		if err != nil {
			fmt.Printf("Write failed: %v\n", err)
			return
		}
		fmt.Printf("Wrote %d bytes to %s\n", totalWritten, path)
	} else if len(args) > 1 && args[1] != "<" {
		// Inline data (typically small)
		writeData := []byte(strings.Join(args[1:], " "))
		if len(writeData) == 0 {
			fmt.Println("No data to write")
			return
		}
		totalWritten, err := writeData_inline(ctx, path, writeData)
		if err != nil {
			fmt.Printf("Write failed: %v\n", err)
			return
		}
		fmt.Printf("Wrote %d bytes to %s\n", totalWritten, path)
	} else {
		fmt.Println("Usage: write <path> <data>  OR  write <path> < localfile")
		return
	}
}

// writeFromFile streams data from a local file to GFS without loading it all into memory
func writeFromFile(_ context.Context, gfsPath, localPath string) (int64, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size for progress reporting
	stat, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}
	totalSize := stat.Size()

	// Start progress bar for larger files (> 1MB)
	var progress *TransferProgress
	if totalSize > 1024*1024 {
		progress = NewTransferProgress(totalSize, "Uploading")
		progress.Start()
		defer progress.Finish()
	}

	var totalWritten int64
	buf := make([]byte, maxChunkSize) // Reusable buffer

	// Per-chunk timeout - resets on each successful chunk write
	const chunkTimeout = 60 * time.Second

	for totalWritten < totalSize {
		// Create fresh context for each chunk operation
		chunkCtx, cancel := context.WithTimeout(context.Background(), chunkTimeout)

		// Check for existing chunks with space
		var chunk *pb.ChunkLocationInfo
		var writeSize int64

		locResp, err := client.GetChunkLocations(chunkCtx, &pb.GetChunkLocationsRequest{Path: gfsPath})
		if err == nil && locResp.Success && len(locResp.Chunks) > 0 {
			lastChunk := locResp.Chunks[len(locResp.Chunks)-1]
			spaceAvailable := int64(maxChunkSize - lastChunk.Size)
			if spaceAvailable > 0 {
				chunk = lastChunk
				writeSize = min(totalSize-totalWritten, spaceAvailable)
			}
		}

		// Allocate new chunk if needed
		if chunk == nil {
			allocResp, err := client.AllocateChunk(chunkCtx, &pb.AllocateChunkRequest{Path: gfsPath})
			if err != nil {
				cancel()
				return totalWritten, fmt.Errorf("failed to allocate chunk: %w", err)
			}
			if !allocResp.Success {
				cancel()
				return totalWritten, fmt.Errorf("chunk allocation failed: %s", allocResp.Message)
			}
			chunk = allocResp.Chunk
			writeSize = min(totalSize-totalWritten, maxChunkSize)
		}
		cancel() // Done with gRPC calls for this chunk

		// Read data from file into buffer
		n, err := io.ReadFull(file, buf[:writeSize])
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return totalWritten, fmt.Errorf("failed to read from file: %w", err)
		}
		if n == 0 {
			break
		}

		// Convert to internal types
		primary := csstructs.ReplicaIdentifier{
			ID:              chunk.Primary.ServerId,
			Hostname:        chunk.Primary.Hostname,
			DataPort:        int(chunk.Primary.DataPort),
			ReplicationPort: int(chunk.Primary.ReplicationPort),
		}

		var replicas []csstructs.ReplicaIdentifier
		for _, loc := range chunk.Locations {
			if loc.ServerId != chunk.Primary.ServerId {
				replicas = append(replicas, csstructs.ReplicaIdentifier{
					ID:              loc.ServerId,
					Hostname:        loc.Hostname,
					DataPort:        int(loc.DataPort),
					ReplicationPort: int(loc.ReplicationPort),
				})
			}
		}

		if err := performWrite(primary, replicas, chunk.ChunkHandle, buf[:n], -1); err != nil {
			return totalWritten, err
		}

		totalWritten += int64(n)

		// Update progress bar or print status
		if progress != nil {
			progress.Update(totalWritten)
		}
	}

	return totalWritten, nil
}

// writeData_inline writes small inline data (for string arguments)
func writeData_inline(_ context.Context, gfsPath string, data []byte) (int, error) {
	totalWritten := 0
	remaining := data

	// Per-chunk timeout - resets on each successful chunk write
	const chunkTimeout = 60 * time.Second

	for len(remaining) > 0 {
		// Create fresh context for each chunk operation
		chunkCtx, cancel := context.WithTimeout(context.Background(), chunkTimeout)

		var chunkData []byte
		var chunk *pb.ChunkLocationInfo

		// Check for existing chunks with space
		locResp, err := client.GetChunkLocations(chunkCtx, &pb.GetChunkLocationsRequest{Path: gfsPath})
		if err == nil && locResp.Success && len(locResp.Chunks) > 0 {
			lastChunk := locResp.Chunks[len(locResp.Chunks)-1]
			spaceAvailable := maxChunkSize - lastChunk.Size
			if spaceAvailable > 0 {
				chunk = lastChunk
				writeSize := uint64(len(remaining))
				if writeSize > spaceAvailable {
					writeSize = spaceAvailable
				}
				chunkData = remaining[:writeSize]
				remaining = remaining[writeSize:]
			}
		}

		// Allocate new chunk if needed
		if chunk == nil {
			allocResp, err := client.AllocateChunk(chunkCtx, &pb.AllocateChunkRequest{Path: gfsPath})
			if err != nil {
				cancel()
				return totalWritten, fmt.Errorf("failed to allocate chunk: %w", err)
			}
			if !allocResp.Success {
				cancel()
				return totalWritten, fmt.Errorf("chunk allocation failed: %s", allocResp.Message)
			}
			chunk = allocResp.Chunk

			writeSize := len(remaining)
			if writeSize > maxChunkSize {
				writeSize = maxChunkSize
			}
			chunkData = remaining[:writeSize]
			remaining = remaining[writeSize:]
		}
		cancel() // Done with gRPC calls for this chunk

		// Convert to internal types
		primary := csstructs.ReplicaIdentifier{
			ID:              chunk.Primary.ServerId,
			Hostname:        chunk.Primary.Hostname,
			DataPort:        int(chunk.Primary.DataPort),
			ReplicationPort: int(chunk.Primary.ReplicationPort),
		}

		var replicas []csstructs.ReplicaIdentifier
		for _, loc := range chunk.Locations {
			if loc.ServerId != chunk.Primary.ServerId {
				replicas = append(replicas, csstructs.ReplicaIdentifier{
					ID:              loc.ServerId,
					Hostname:        loc.Hostname,
					DataPort:        int(loc.DataPort),
					ReplicationPort: int(loc.ReplicationPort),
				})
			}
		}

		if err := performWrite(primary, replicas, chunk.ChunkHandle, chunkData, -1); err != nil {
			return totalWritten, err
		}

		totalWritten += len(chunkData)
		if len(remaining) > 0 {
			fmt.Printf("  Wrote chunk %s (%d bytes), %d bytes remaining...\n", chunk.ChunkHandle, len(chunkData), len(remaining))
		}
	}

	return totalWritten, nil
}

// ============ rm command ============

func cmdRm(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: rm <path>")
		return
	}
	path := args[0]

	ctx, cancel := getContext()
	defer cancel()

	resp, err := client.DeleteFile(ctx, &pb.DeleteFileRequest{Path: path})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	if !resp.Success {
		fmt.Printf("Error: %s\n", resp.Message)
		return
	}

	fmt.Printf("Deleted %s\n", path)
}

// ============ mv command ============

func cmdMv(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: mv <source> <destination>")
		return
	}
	oldPath := args[0]
	newPath := args[1]

	ctx, cancel := getContext()
	defer cancel()

	resp, err := client.RenameFile(ctx, &pb.RenameFileRequest{OldPath: oldPath, NewPath: newPath})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	if !resp.Success {
		fmt.Printf("Error: %s\n", resp.Message)
		return
	}

	fmt.Printf("Renamed %s -> %s\n", oldPath, newPath)
}

// ============ info command ============

func cmdInfo(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: info <path>")
		return
	}
	path := args[0]

	ctx, cancel := getContext()
	defer cancel()

	// Get file info
	fileResp, err := client.GetFile(ctx, &pb.GetFileRequest{Path: path})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	if !fileResp.Success {
		fmt.Printf("Error: %s\n", fileResp.Message)
		return
	}

	f := fileResp.File
	fmt.Printf("Path:       %s\n", f.Path)
	fmt.Printf("Size:       %d bytes\n", f.Size)
	fmt.Printf("Chunk Size: %d bytes\n", f.ChunkSize)
	fmt.Printf("Chunks:     %d\n", len(f.ChunkHandles))

	// Get chunk locations
	locResp, err := client.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: path})
	if err == nil && locResp.Success && len(locResp.Chunks) > 0 {
		fmt.Println("\nChunk Details:")
		for i, chunk := range locResp.Chunks {
			fmt.Printf("  [%d] %s (%d bytes)\n", i, chunk.ChunkHandle, chunk.Size)
			if chunk.Primary != nil {
				fmt.Printf("      Primary: %s:%d\n", chunk.Primary.Hostname, chunk.Primary.DataPort)
			}
			for _, loc := range chunk.Locations {
				if chunk.Primary == nil || loc.ServerId != chunk.Primary.ServerId {
					fmt.Printf("      Replica: %s:%d\n", loc.Hostname, loc.DataPort)
				}
			}
		}
	}
}

// ============ pressure command ============

func cmdPressure(args []string) {
	ctx, cancel := getContext()
	defer cancel()

	resp, err := client.GetClusterPressure(ctx, &pb.GetClusterPressureRequest{})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Cluster Status")
	fmt.Println("==============")
	fmt.Println()

	// Show master build info
	if resp.MasterBuildInfo != nil {
		fmt.Printf("Master Build: %s (%s)\n", resp.MasterBuildInfo.BuildId, resp.MasterBuildInfo.BuildTime)
	}
	fmt.Println()

	if len(resp.Servers) == 0 {
		fmt.Println("No chunkservers registered")
		return
	}

	for _, server := range resp.Servers {
		status := "ALIVE"
		if !server.IsAlive {
			status = "DEAD"
		}

		fmt.Printf("Server: %s (%s:%d) [%s]\n",
			server.Server.ServerId,
			server.Server.Hostname,
			server.Server.DataPort,
			status)

		// Show build info
		if server.BuildInfo != nil {
			fmt.Printf("  Build:  %s (%s)\n", server.BuildInfo.BuildId, server.BuildInfo.BuildTime)
		}
		fmt.Printf("  Chunks: %d\n", server.ChunkCount)

		if server.Resources != nil {
			r := server.Resources

			// CPU bar
			cpuBar := progressBar(r.CpuUsagePercent, 20)
			fmt.Printf("  CPU:    [%s] %5.1f%%\n", cpuBar, r.CpuUsagePercent)

			// Memory bar and details
			memBar := progressBar(r.MemoryUsagePercent, 20)
			memUsedGB := float64(r.MemoryUsedBytes) / (1024 * 1024 * 1024)
			memTotalGB := float64(r.MemoryTotalBytes) / (1024 * 1024 * 1024)
			fmt.Printf("  Memory: [%s] %5.1f%% (%.1f/%.1f GB)\n",
				memBar, r.MemoryUsagePercent, memUsedGB, memTotalGB)

			// Disk bar and details
			diskBar := progressBar(r.DiskUsagePercent, 20)
			diskUsedGB := float64(r.DiskUsedBytes) / (1024 * 1024 * 1024)
			diskTotalGB := float64(r.DiskTotalBytes) / (1024 * 1024 * 1024)
			fmt.Printf("  Disk:   [%s] %5.1f%% (%.1f/%.1f GB)\n",
				diskBar, r.DiskUsagePercent, diskUsedGB, diskTotalGB)
		} else {
			fmt.Println("  (no resource data available)")
		}
		fmt.Println()
	}
}

// progressBar creates a visual progress bar
func progressBar(percent float64, width int) string {
	filled := int(percent / 100 * float64(width))
	if filled > width {
		filled = width
	}
	if filled < 0 {
		filled = 0
	}

	bar := make([]byte, width)
	for i := 0; i < width; i++ {
		if i < filled {
			bar[i] = '#'
		} else {
			bar[i] = '-'
		}
	}
	return string(bar)
}

// ============ Data operations ============

func performWrite(primary csstructs.ReplicaIdentifier, replicas []csstructs.ReplicaIdentifier, chunkHandle string, data []byte, offset int64) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", primary.Hostname, primary.DataPort))
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Send action type (Download = write to chunkserver)
	actionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(actionBytes, uint32(csstructs.Download))
	if _, err = conn.Write(actionBytes); err != nil {
		return fmt.Errorf("failed to send action: %w", err)
	}

	// Create JWT with write metadata
	claims := csstructs.DownloadRequestClaims{
		ChunkHandle: chunkHandle,
		Operation:   "download",
		Filesize:    uint64(len(data)),
		Offset:      offset,
		Replicas:    replicas,
		Primary:     primary,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	secret, err := secrets.GetSecret(nil)
	if err != nil {
		return fmt.Errorf("failed to get secret: %w", err)
	}

	tokenString, err := token.SignedString(secret)
	if err != nil {
		return fmt.Errorf("failed to sign token: %w", err)
	}

	// Send JWT token
	tokenLen := int32(len(tokenString))
	if err = binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
		return fmt.Errorf("failed to send token length: %w", err)
	}

	if _, err = conn.Write([]byte(tokenString)); err != nil {
		return fmt.Errorf("failed to send token: %w", err)
	}

	// Wait for offset allocation
	offsetBytes := make([]byte, 8)
	if _, err = conn.Read(offsetBytes); err != nil {
		return fmt.Errorf("failed to receive offset: %w", err)
	}

	// Send data payload
	if _, err = conn.Write(data); err != nil {
		return fmt.Errorf("failed to send data: %w", err)
	}

	// Close write side to signal EOF
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.CloseWrite()
	}

	// Wait for final commit response
	resultBytes := make([]byte, 1)
	if _, err = conn.Read(resultBytes); err != nil {
		return fmt.Errorf("failed to receive commit response: %w", err)
	}

	if resultBytes[0] == 1 {
		return nil
	}

	return fmt.Errorf("data persistence failed")
}

// performReadStream streams chunk data directly to the provided writer.
// Returns the number of bytes written and any error encountered.
func performReadStream(server csstructs.ReplicaIdentifier, chunkHandle string, w io.Writer) (int64, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", server.Hostname, server.DataPort))
	if err != nil {
		return 0, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Send Upload action (read from chunkserver)
	actionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(actionBytes, uint32(csstructs.Upload))
	if _, err = conn.Write(actionBytes); err != nil {
		return 0, fmt.Errorf("failed to send action: %w", err)
	}

	// Create read JWT
	claims := csstructs.UploadRequestClaims{
		ChunkHandle: chunkHandle,
		Operation:   "upload",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	secret, err := secrets.GetSecret(nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get secret: %w", err)
	}

	tokenString, err := token.SignedString(secret)
	if err != nil {
		return 0, fmt.Errorf("failed to sign token: %w", err)
	}

	// Send JWT
	tokenLen := int32(len(tokenString))
	if err = binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
		return 0, fmt.Errorf("failed to send token length: %w", err)
	}
	if _, err = conn.Write([]byte(tokenString)); err != nil {
		return 0, fmt.Errorf("failed to send token: %w", err)
	}

	// Read response status
	statusBytes := make([]byte, 1)
	if _, err = conn.Read(statusBytes); err != nil {
		return 0, fmt.Errorf("failed to read status: %w", err)
	}

	if statusBytes[0] == 0 {
		// Read error details
		codeBytes := make([]byte, 4)
		conn.Read(codeBytes)
		errorCode := binary.BigEndian.Uint32(codeBytes)

		lenBytes := make([]byte, 4)
		conn.Read(lenBytes)
		msgLen := binary.BigEndian.Uint32(lenBytes)

		msgBytes := make([]byte, msgLen)
		conn.Read(msgBytes)

		return 0, fmt.Errorf("read failed: code=%d, message=%s", errorCode, string(msgBytes))
	}

	// Read file size
	sizeBytes := make([]byte, 8)
	if _, err = conn.Read(sizeBytes); err != nil {
		return 0, fmt.Errorf("failed to read file size: %w", err)
	}
	fileSize := binary.BigEndian.Uint64(sizeBytes)

	// Stream data directly to writer using io.CopyN with a limited reader
	written, err := io.CopyN(w, conn, int64(fileSize))
	if err != nil && err != io.EOF {
		return written, fmt.Errorf("failed to stream data: %w", err)
	}

	return written, nil
}
