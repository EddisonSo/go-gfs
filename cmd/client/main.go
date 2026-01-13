package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	gfs "eddisonso.com/go-gfs/pkg/go-gfs-sdk"
	"golang.org/x/term"
)

var (
	masterAddr string
	client     *gfs.Client
)

// ============ Progress Bar ============

// TransferProgress tracks and displays transfer progress
type TransferProgress struct {
	total      int64
	current    int64
	startTime  time.Time
	lastUpdate time.Time
	operation  string // "Uploading" or "Downloading"
	done       chan struct{}
	mu         sync.Mutex
	isTerminal bool
	termWidth  int
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

func main() {
	masterAddr = "localhost:9000"

	// Check for -master flag
	for i, arg := range os.Args[1:] {
		if arg == "-master" && i+2 < len(os.Args) {
			masterAddr = os.Args[i+2]
		}
	}

	// Connect to master using SDK
	fmt.Printf("Connecting to master at %s...\n", masterAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	var err error
	client, err = gfs.New(ctx, masterAddr)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to master: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Verify connection by making a test call
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = client.ListFiles(ctx, "")
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
  write [--namespace <name>] <path> <data>   Write data to file
  write [--namespace <name>] <path> < <file> Write local file to GFS
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

	files, err := client.ListFiles(ctx, prefix)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Println("No files found")
		return
	}

	for _, f := range files {
		fmt.Printf("%s\t%d chunks\t%d bytes\n", f.Path, len(f.ChunkHandles), f.Size)
	}
}

// ============ read command ============

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

	// Get file info for size (for progress bar)
	var totalSize int64
	if fileInfo, err := client.GetFile(ctx, path); err == nil {
		totalSize = int64(fileInfo.Size)
	}

	// Set up output writer
	var output io.Writer
	var outFile *os.File
	var err error
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

	// Start progress bar for larger files (> 1MB) when writing to file
	var progress *TransferProgress
	if totalSize > 1024*1024 && outputFile != "" {
		progress = NewTransferProgress(totalSize, "Downloading")
		progress.Start()
		defer progress.Finish()
		output = &ProgressWriter{w: output, progress: progress}
	}

	// Read using SDK
	n, err := client.ReadTo(ctx, path, output)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Add newline to stdout if needed (for text files)
	if outputFile == "" {
		fmt.Println()
	} else if progress == nil {
		// Only print message if we didn't show a progress bar
		fmt.Printf("Wrote %d bytes to %s\n", n, outputFile)
	}
}

// ============ write command ============

func cmdWrite(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: write [--namespace <name>] <path> <data>  OR  write [--namespace <name>] <path> < localfile")
		return
	}

	namespace, remaining, err := extractNamespace(args)
	if err != nil {
		fmt.Printf("Usage error: %v\n", err)
		return
	}
	if len(remaining) < 1 {
		fmt.Println("Usage: write [--namespace <name>] <path> <data>  OR  write [--namespace <name>] <path> < localfile")
		return
	}

	path := remaining[0]

	// Check for < redirect
	inputFile := ""
	for i, arg := range remaining {
		if arg == "<" && i+1 < len(remaining) {
			inputFile = remaining[i+1]
			break
		}
	}

	ctx, cancel := getContext()
	defer cancel()

	// Create file if it doesn't exist
	client.CreateFileWithNamespace(ctx, path, namespace)

	if inputFile != "" {
		// Stream from file using SDK
		totalWritten, err := writeFromFile(ctx, path, inputFile, namespace)
		if err != nil {
			fmt.Printf("Write failed: %v\n", err)
			return
		}
		fmt.Printf("Wrote %d bytes to %s\n", totalWritten, path)
	} else if len(remaining) > 1 && remaining[1] != "<" {
		// Inline data (typically small)
		writeData := []byte(strings.Join(remaining[1:], " "))
		if len(writeData) == 0 {
			fmt.Println("No data to write")
			return
		}
		n, err := client.Write(ctx, path, writeData)
		if err != nil {
			fmt.Printf("Write failed: %v\n", err)
			return
		}
		fmt.Printf("Wrote %d bytes to %s\n", n, path)
	} else {
		fmt.Println("Usage: write [--namespace <name>] <path> <data>  OR  write [--namespace <name>] <path> < localfile")
		return
	}
}

// writeFromFile streams data from a local file to GFS using the SDK
func writeFromFile(ctx context.Context, gfsPath, localPath, namespace string) (int64, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size for progress reporting and pre-allocation
	stat, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}
	totalSize := stat.Size()

	// Pre-allocate chunks to avoid delays during upload
	prepared, err := client.PrepareUploadWithNamespace(ctx, gfsPath, namespace, totalSize)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare upload: %w", err)
	}

	// Start progress bar for larger files (> 1MB)
	var progress *TransferProgress
	var reader io.Reader = file
	if totalSize > 1024*1024 {
		progress = NewTransferProgress(totalSize, "Uploading")
		progress.Start()
		defer progress.Finish()
		reader = &ProgressReader{r: file, progress: progress}
	}

	// Use prepared upload to stream the file (no allocation delays)
	return prepared.AppendFrom(ctx, reader)
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

	if err := client.DeleteFile(ctx, path); err != nil {
		fmt.Printf("Error: %v\n", err)
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

	if err := client.RenameFile(ctx, oldPath, newPath); err != nil {
		fmt.Printf("Error: %v\n", err)
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
	f, err := client.GetFile(ctx, path)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Path:       %s\n", f.Path)
	namespace := f.Namespace
	if namespace == "" {
		namespace = gfs.DefaultNamespace
	}
	fmt.Printf("Namespace:  %s\n", namespace)
	fmt.Printf("Size:       %d bytes\n", f.Size)
	fmt.Printf("Chunk Size: %d bytes\n", f.ChunkSize)
	fmt.Printf("Chunks:     %d\n", len(f.ChunkHandles))

	// Get chunk locations
	chunks, err := client.GetChunkLocations(ctx, path)
	if err == nil && len(chunks) > 0 {
		fmt.Println("\nChunk Details:")
		for i, chunk := range chunks {
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

func extractNamespace(args []string) (string, []string, error) {
	var namespace string
	remaining := make([]string, 0, len(args))

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--namespace" || arg == "-n" {
			if i+1 >= len(args) {
				return "", nil, fmt.Errorf("missing namespace value")
			}
			namespace = args[i+1]
			i++
			continue
		}
		if strings.HasPrefix(arg, "--namespace=") {
			namespace = strings.TrimPrefix(arg, "--namespace=")
			continue
		}
		if strings.HasPrefix(arg, "-n=") {
			namespace = strings.TrimPrefix(arg, "-n=")
			continue
		}
		remaining = append(remaining, arg)
	}

	return namespace, remaining, nil
}

// ============ pressure command ============

func cmdPressure(args []string) {
	ctx, cancel := getContext()
	defer cancel()

	resp, err := client.GetClusterPressure(ctx)
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
