package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	pb "eddisonso.com/go-gfs/gen/master"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var masterAddr string

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Global flags
	globalFlags := flag.NewFlagSet("global", flag.ExitOnError)
	globalFlags.StringVar(&masterAddr, "master", "localhost:9000", "Master server address")

	// Find the command position (skip global flags)
	cmdIdx := 1
	for cmdIdx < len(os.Args) && os.Args[cmdIdx][0] == '-' {
		cmdIdx++
		// Skip flag value if it's a separate arg
		if cmdIdx < len(os.Args) && os.Args[cmdIdx-1] != "-master" {
			continue
		}
		if cmdIdx < len(os.Args) && os.Args[cmdIdx][0] != '-' {
			cmdIdx++
		}
	}

	// Parse global flags before command
	if cmdIdx > 1 {
		globalFlags.Parse(os.Args[1:cmdIdx])
	}

	if cmdIdx >= len(os.Args) {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[cmdIdx]
	args := os.Args[cmdIdx+1:]

	switch cmd {
	case "ls":
		cmdLs(args)
	case "read", "cat":
		cmdRead(args)
	case "write":
		cmdWrite(args)
	case "rm":
		cmdRm(args)
	case "info":
		cmdInfo(args)
	case "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`GFS Client - Google File System CLI

Usage:
  gfs [global flags] <command> [arguments]

Global Flags:
  -master string    Master server address (default "localhost:9000")

Commands:
  ls                List all files
  read <path>       Read a file from GFS
  write <path>      Write data to a file in GFS
  rm <path>         Delete a file
  info <path>       Show file information
  help              Show this help message

Examples:
  gfs ls
  gfs write /myfile.txt -data "Hello World"
  gfs write /myfile.txt -input localfile.txt
  gfs read /myfile.txt
  gfs read /myfile.txt -output localfile.txt
  gfs rm /myfile.txt
  gfs info /myfile.txt`)
}

func connectMaster() (pb.MasterClient, *grpc.ClientConn, context.Context, context.CancelFunc) {
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to master: %v\n", err)
		os.Exit(1)
	}

	client := pb.NewMasterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	return client, conn, ctx, cancel
}

// ============ ls command ============

func cmdLs(args []string) {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	prefix := fs.String("prefix", "", "Filter by path prefix")
	fs.Parse(args)

	client, conn, ctx, cancel := connectMaster()
	defer conn.Close()
	defer cancel()

	resp, err := client.ListFiles(ctx, &pb.ListFilesRequest{Prefix: *prefix})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to list files: %v\n", err)
		os.Exit(1)
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
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	output := fs.String("output", "", "Write to local file instead of stdout")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: gfs read <path> [-output file]")
		os.Exit(1)
	}
	path := fs.Arg(0)

	client, conn, ctx, cancel := connectMaster()
	defer conn.Close()
	defer cancel()

	// Get chunk locations
	locResp, err := client.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: path})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get chunk locations: %v\n", err)
		os.Exit(1)
	}
	if !locResp.Success {
		fmt.Fprintf(os.Stderr, "Error: %s\n", locResp.Message)
		os.Exit(1)
	}

	if len(locResp.Chunks) == 0 {
		fmt.Fprintln(os.Stderr, "No chunks found for file")
		os.Exit(1)
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
			fmt.Fprintf(os.Stderr, "No available servers for chunk %s\n", chunk.ChunkHandle)
			os.Exit(1)
		}

		replica := csstructs.ReplicaIdentifier{
			ID:              server.ServerId,
			Hostname:        server.Hostname,
			DataPort:        int(server.DataPort),
			ReplicationPort: int(server.ReplicationPort),
		}

		data, err := performRead(replica, chunk.ChunkHandle)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read chunk %s: %v\n", chunk.ChunkHandle, err)
			os.Exit(1)
		}
		allData = append(allData, data...)
	}

	if *output != "" {
		if err := os.WriteFile(*output, allData, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write output file: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Wrote %d bytes to %s\n", len(allData), *output)
	} else {
		os.Stdout.Write(allData)
	}
}

// ============ write command ============

func cmdWrite(args []string) {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	data := fs.String("data", "", "Data string to write")
	input := fs.String("input", "", "Read data from local file")
	offset := fs.Int64("offset", -1, "Write at specific offset (-1 for append)")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: gfs write <path> [-data string | -input file] [-offset n]")
		os.Exit(1)
	}
	path := fs.Arg(0)

	// Get write data
	var writeData []byte
	if *input != "" {
		var err error
		writeData, err = os.ReadFile(*input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read input file: %v\n", err)
			os.Exit(1)
		}
	} else if *data != "" {
		writeData = []byte(*data)
	} else {
		// Read from stdin
		var err error
		writeData, err = io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read from stdin: %v\n", err)
			os.Exit(1)
		}
	}

	if len(writeData) == 0 {
		fmt.Fprintln(os.Stderr, "No data to write")
		os.Exit(1)
	}

	client, conn, ctx, cancel := connectMaster()
	defer conn.Close()
	defer cancel()

	// Create file if it doesn't exist
	client.CreateFile(ctx, &pb.CreateFileRequest{Path: path})

	// Check for existing chunks with space
	var chunk *pb.ChunkLocationInfo
	const maxChunkSize = 64 << 20 // 64MB

	locResp, err := client.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: path})
	if err == nil && locResp.Success && len(locResp.Chunks) > 0 {
		lastChunk := locResp.Chunks[len(locResp.Chunks)-1]
		if lastChunk.Size+uint64(len(writeData)) <= maxChunkSize {
			chunk = lastChunk
		}
	}

	// Allocate new chunk if needed
	if chunk == nil {
		allocResp, err := client.AllocateChunk(ctx, &pb.AllocateChunkRequest{Path: path})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to allocate chunk: %v\n", err)
			os.Exit(1)
		}
		if !allocResp.Success {
			fmt.Fprintf(os.Stderr, "Chunk allocation failed: %s\n", allocResp.Message)
			os.Exit(1)
		}
		chunk = allocResp.Chunk
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

	if err := performWrite(primary, replicas, chunk.ChunkHandle, writeData, *offset); err != nil {
		fmt.Fprintf(os.Stderr, "Write failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Wrote %d bytes to %s\n", len(writeData), path)
}

// ============ rm command ============

func cmdRm(args []string) {
	fs := flag.NewFlagSet("rm", flag.ExitOnError)
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: gfs rm <path>")
		os.Exit(1)
	}
	path := fs.Arg(0)

	client, conn, ctx, cancel := connectMaster()
	defer conn.Close()
	defer cancel()

	resp, err := client.DeleteFile(ctx, &pb.DeleteFileRequest{Path: path})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to delete file: %v\n", err)
		os.Exit(1)
	}
	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Message)
		os.Exit(1)
	}

	fmt.Printf("Deleted %s\n", path)
}

// ============ info command ============

func cmdInfo(args []string) {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: gfs info <path>")
		os.Exit(1)
	}
	path := fs.Arg(0)

	client, conn, ctx, cancel := connectMaster()
	defer conn.Close()
	defer cancel()

	// Get file info
	fileResp, err := client.GetFile(ctx, &pb.GetFileRequest{Path: path})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get file: %v\n", err)
		os.Exit(1)
	}
	if !fileResp.Success {
		fmt.Fprintf(os.Stderr, "Error: %s\n", fileResp.Message)
		os.Exit(1)
	}

	f := fileResp.File
	fmt.Printf("Path:       %s\n", f.Path)
	fmt.Printf("Size:       %d bytes\n", f.Size)
	fmt.Printf("Chunk Size: %d bytes\n", f.ChunkSize)
	fmt.Printf("Chunks:     %d\n", len(f.ChunkHandles))

	// Get chunk locations
	locResp, err := client.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: path})
	if err == nil && locResp.Success {
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
