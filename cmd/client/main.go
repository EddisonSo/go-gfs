package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	pb "eddisonso.com/go-gfs/gen/master"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	masterAddr string
	client     pb.MasterClient
	conn       *grpc.ClientConn
)

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
		fmt.Fprintf(os.Stderr, "Failed to connect to master: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client = pb.NewMasterClient(conn)

	fmt.Printf("GFS Client - Connected to %s\n", masterAddr)
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
		case "rm":
			cmdRm(cmdArgs)
		case "info":
			cmdInfo(cmdArgs)
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
  rm <path>               Delete a file
  info <path>             Show file information
  help                    Show this help
  exit                    Quit the client

Examples:
  ls
  write /hello.txt "Hello World"
  read /hello.txt
  info /hello.txt
  rm /hello.txt`)
}

func getContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
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

	// Read all chunks and concatenate
	var allData []byte
	for _, chunk := range locResp.Chunks {
		var server *pb.ChunkServerInfo
		if chunk.Primary != nil {
			server = chunk.Primary
		} else if len(chunk.Locations) > 0 {
			server = chunk.Locations[0]
		} else {
			fmt.Printf("No available servers for chunk %s\n", chunk.ChunkHandle)
			return
		}

		replica := csstructs.ReplicaIdentifier{
			ID:              server.ServerId,
			Hostname:        server.Hostname,
			DataPort:        int(server.DataPort),
			ReplicationPort: int(server.ReplicationPort),
		}

		data, err := performRead(replica, chunk.ChunkHandle)
		if err != nil {
			fmt.Printf("Failed to read chunk %s: %v\n", chunk.ChunkHandle, err)
			return
		}
		allData = append(allData, data...)
	}

	if outputFile != "" {
		if err := os.WriteFile(outputFile, allData, 0644); err != nil {
			fmt.Printf("Failed to write file: %v\n", err)
			return
		}
		fmt.Printf("Wrote %d bytes to %s\n", len(allData), outputFile)
	} else {
		fmt.Print(string(allData))
		if len(allData) > 0 && allData[len(allData)-1] != '\n' {
			fmt.Println()
		}
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
	var writeData []byte

	// Check for < redirect
	inputFile := ""
	for i, arg := range args {
		if arg == "<" && i+1 < len(args) {
			inputFile = args[i+1]
			break
		}
	}

	if inputFile != "" {
		var err error
		writeData, err = os.ReadFile(inputFile)
		if err != nil {
			fmt.Printf("Failed to read file: %v\n", err)
			return
		}
	} else if len(args) > 1 && args[1] != "<" {
		// Join remaining args as data
		writeData = []byte(strings.Join(args[1:], " "))
	} else {
		fmt.Println("Usage: write <path> <data>  OR  write <path> < localfile")
		return
	}

	if len(writeData) == 0 {
		fmt.Println("No data to write")
		return
	}

	ctx, cancel := getContext()
	defer cancel()

	// Create file if it doesn't exist
	client.CreateFile(ctx, &pb.CreateFileRequest{Path: path})

	totalWritten := 0
	remaining := writeData

	for len(remaining) > 0 {
		// Determine how much to write in this chunk
		var chunkData []byte
		var chunk *pb.ChunkLocationInfo

		// Check for existing chunks with space
		locResp, err := client.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: path})
		if err == nil && locResp.Success && len(locResp.Chunks) > 0 {
			lastChunk := locResp.Chunks[len(locResp.Chunks)-1]
			spaceAvailable := maxChunkSize - lastChunk.Size
			if spaceAvailable > 0 {
				chunk = lastChunk
				// Write only what fits
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
			allocResp, err := client.AllocateChunk(ctx, &pb.AllocateChunkRequest{Path: path})
			if err != nil {
				fmt.Printf("Failed to allocate chunk: %v\n", err)
				return
			}
			if !allocResp.Success {
				fmt.Printf("Chunk allocation failed: %s\n", allocResp.Message)
				return
			}
			chunk = allocResp.Chunk

			// Write up to maxChunkSize
			writeSize := len(remaining)
			if writeSize > maxChunkSize {
				writeSize = maxChunkSize
			}
			chunkData = remaining[:writeSize]
			remaining = remaining[writeSize:]
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

		if err := performWrite(primary, replicas, chunk.ChunkHandle, chunkData, -1); err != nil {
			fmt.Printf("Write failed: %v\n", err)
			return
		}

		totalWritten += len(chunkData)
		if len(remaining) > 0 {
			fmt.Printf("  Wrote chunk %s (%d bytes), %d bytes remaining...\n", chunk.ChunkHandle, len(chunkData), len(remaining))
		}
	}

	fmt.Printf("Wrote %d bytes to %s\n", totalWritten, path)
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

func performRead(server csstructs.ReplicaIdentifier, chunkHandle string) ([]byte, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", server.Hostname, server.DataPort))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Send Upload action (read from chunkserver)
	actionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(actionBytes, uint32(csstructs.Upload))
	if _, err = conn.Write(actionBytes); err != nil {
		return nil, fmt.Errorf("failed to send action: %w", err)
	}

	// Create read JWT
	claims := csstructs.UploadRequestClaims{
		ChunkHandle: chunkHandle,
		Operation:   "upload",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	secret, err := secrets.GetSecret(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get secret: %w", err)
	}

	tokenString, err := token.SignedString(secret)
	if err != nil {
		return nil, fmt.Errorf("failed to sign token: %w", err)
	}

	// Send JWT
	tokenLen := int32(len(tokenString))
	if err = binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
		return nil, fmt.Errorf("failed to send token length: %w", err)
	}
	if _, err = conn.Write([]byte(tokenString)); err != nil {
		return nil, fmt.Errorf("failed to send token: %w", err)
	}

	// Read response status
	statusBytes := make([]byte, 1)
	if _, err = conn.Read(statusBytes); err != nil {
		return nil, fmt.Errorf("failed to read status: %w", err)
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

		return nil, fmt.Errorf("read failed: code=%d, message=%s", errorCode, string(msgBytes))
	}

	// Read file size
	sizeBytes := make([]byte, 8)
	if _, err = conn.Read(sizeBytes); err != nil {
		return nil, fmt.Errorf("failed to read file size: %w", err)
	}
	fileSize := binary.BigEndian.Uint64(sizeBytes)

	// Read file contents
	data := make([]byte, fileSize)
	totalRead := 0
	for totalRead < int(fileSize) {
		n, err := conn.Read(data[totalRead:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read data: %w", err)
		}
		totalRead += n
	}

	return data[:totalRead], nil
}
