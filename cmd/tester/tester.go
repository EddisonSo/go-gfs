package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
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

var (
	doWrite     bool
	doRead      bool
	doList      bool
	inputFile   string
	outputFile  string
	filePath    string
	data        string
	writeOffset int64
	masterAddr  string

	// Legacy flags for standalone mode (no master)
	chunkHandle string
	host        string
	port        int
)

func init() {
	flag.BoolVar(&doWrite, "write", false, "Perform write operation")
	flag.BoolVar(&doRead, "read", false, "Perform read operation")
	flag.BoolVar(&doList, "ls", false, "List all files")
	flag.StringVar(&inputFile, "input", "", "Input file for write operation (reads from file instead of --data)")
	flag.StringVar(&outputFile, "output", "", "Output file for read operation (writes chunk data to file)")
	flag.StringVar(&filePath, "file", "", "File path in GFS namespace (required with -master)")
	flag.StringVar(&data, "data", "hello", "Data to write (used if --input not specified)")
	flag.Int64Var(&writeOffset, "offset", -1, "Write at specific offset (-1 for append)")
	flag.StringVar(&masterAddr, "master", "", "Master server address (e.g., localhost:9000). Required for normal operation.")

	// Legacy flags for standalone mode
	flag.StringVar(&chunkHandle, "chunk", "", "Chunk handle (standalone mode only, bypasses master)")
	flag.StringVar(&host, "host", "localhost", "Chunkserver hostname (standalone mode only)")
	flag.IntVar(&port, "port", 8080, "Chunkserver data port (standalone mode only)")
}

func main() {
	flag.Parse()

	// If neither read nor write specified, run both
	if !doWrite && !doRead {
		doWrite = true
		doRead = true
	}

	var writeData []byte
	if doWrite {
		if inputFile != "" {
			fileData, err := os.ReadFile(inputFile)
			if err != nil {
				log.Fatalf("Failed to read input file: %v", err)
			}
			writeData = fileData
			log.Printf("Read %d bytes from %s", len(writeData), inputFile)
		} else {
			writeData = []byte(data)
		}
	}

	// Check if using master or standalone mode
	if masterAddr != "" {
		if filePath == "" && !doList {
			log.Fatal("Must specify -file when using -master mode (except for -ls)")
		}
		runWithMaster(writeData)
	} else if chunkHandle != "" {
		runStandalone(writeData)
	} else {
		log.Fatal("Must specify either -master (for normal operation) or -chunk (for standalone mode)")
	}
}

func runWithMaster(writeData []byte) {
	log.Printf("Connecting to master at %s", masterAddr)

	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// List files if requested
	if doList {
		log.Println("=== File Listing ===")
		listResp, err := client.ListFiles(ctx, &pb.ListFilesRequest{})
		if err != nil {
			log.Fatalf("Failed to list files: %v", err)
		}
		if len(listResp.Files) == 0 {
			log.Println("No files found")
		} else {
			for _, f := range listResp.Files {
				log.Printf("  %s (%d chunks, %d bytes)", f.Path, len(f.ChunkHandles), f.Size)
			}
		}
		return
	}

	// Create file if it doesn't exist
	log.Printf("Creating file: %s", filePath)
	createResp, err := client.CreateFile(ctx, &pb.CreateFileRequest{Path: filePath})
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	if !createResp.Success {
		// File might already exist, try to get it
		log.Printf("File creation response: %s (may already exist)", createResp.Message)
	} else {
		log.Printf("File created: %s", filePath)
	}

	if doWrite {
		log.Println("")
		log.Println("=== Write Operation ===")

		var chunk *pb.ChunkLocationInfo
		const maxChunkSize = 64 << 20 // 64MB

		// Check if file has existing chunks with space
		locResp, err := client.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: filePath})
		if err == nil && locResp.Success && len(locResp.Chunks) > 0 {
			lastChunk := locResp.Chunks[len(locResp.Chunks)-1]
			if lastChunk.Size+uint64(len(writeData)) <= maxChunkSize {
				log.Printf("Using existing chunk: %s (size: %d bytes)", lastChunk.ChunkHandle, lastChunk.Size)
				chunk = lastChunk
			} else {
				log.Printf("Last chunk full (%d bytes), allocating new chunk", lastChunk.Size)
			}
		}

		// Allocate new chunk if needed
		if chunk == nil {
			log.Printf("Allocating chunk for file: %s", filePath)
			allocResp, err := client.AllocateChunk(ctx, &pb.AllocateChunkRequest{Path: filePath})
			if err != nil {
				log.Fatalf("Failed to allocate chunk: %v", err)
			}
			if !allocResp.Success {
				log.Fatalf("Chunk allocation failed: %s", allocResp.Message)
			}
			chunk = allocResp.Chunk
			log.Printf("Allocated new chunk: %s", chunk.ChunkHandle)
		}

		log.Printf("Primary: %s:%d", chunk.Primary.Hostname, chunk.Primary.DataPort)
		log.Printf("Replicas: %d locations", len(chunk.Locations))

		// Convert protobuf types to internal types
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

		if err := performWrite(primary, replicas, chunk.ChunkHandle, writeData, writeOffset); err != nil {
			log.Fatalf("Write failed: %v", err)
		}
	}

	if doRead {
		log.Println("")
		log.Println("=== Read Operation ===")

		// Get chunk locations for reading
		log.Printf("Getting chunk locations for file: %s", filePath)
		locResp, err := client.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: filePath})
		if err != nil {
			log.Fatalf("Failed to get chunk locations: %v", err)
		}
		if !locResp.Success {
			log.Fatalf("Get chunk locations failed: %s", locResp.Message)
		}

		if len(locResp.Chunks) == 0 {
			log.Fatal("No chunks found for file")
		}

		// Read from first chunk (for simplicity)
		chunk := locResp.Chunks[0]
		log.Printf("Reading chunk: %s", chunk.ChunkHandle)

		// Pick a replica to read from (primary for now)
		var readServer *pb.ChunkServerInfo
		if chunk.Primary != nil {
			readServer = chunk.Primary
		} else if len(chunk.Locations) > 0 {
			readServer = chunk.Locations[0]
		} else {
			log.Fatal("No available servers for chunk")
		}

		primary := csstructs.ReplicaIdentifier{
			ID:              readServer.ServerId,
			Hostname:        readServer.Hostname,
			DataPort:        int(readServer.DataPort),
			ReplicationPort: int(readServer.ReplicationPort),
		}

		readData, err := performRead(primary, chunk.ChunkHandle)
		if err != nil {
			log.Fatalf("Read failed: %v", err)
		}

		if outputFile != "" {
			if err := os.WriteFile(outputFile, readData, 0644); err != nil {
				log.Fatalf("Failed to write output file: %v", err)
			}
			log.Printf("Wrote %d bytes to %s", len(readData), outputFile)
		} else {
			log.Printf("Read data: '%s'", string(readData))
		}
	}

	log.Println("Done")
}

func runStandalone(writeData []byte) {
	log.Println("Running in standalone mode (no master)")

	// Define replica topology manually
	primary := csstructs.ReplicaIdentifier{
		ID:              "replica1",
		Hostname:        host,
		DataPort:        port,
		ReplicationPort: port + 1,
	}

	r2 := csstructs.ReplicaIdentifier{
		ID:              "replica2",
		Hostname:        "chunkserver2",
		DataPort:        8080,
		ReplicationPort: 8081,
	}

	r3 := csstructs.ReplicaIdentifier{
		ID:              "replica3",
		Hostname:        "chunkserver3",
		DataPort:        8080,
		ReplicationPort: 8081,
	}

	replicas := []csstructs.ReplicaIdentifier{r2, r3}

	if doWrite {
		log.Println("=== Write Operation ===")
		if err := performWrite(primary, replicas, chunkHandle, writeData, writeOffset); err != nil {
			log.Fatalf("Write failed: %v", err)
		}
	}

	if doRead {
		log.Println("")
		log.Println("=== Read Operation ===")
		readData, err := performRead(primary, chunkHandle)
		if err != nil {
			log.Fatalf("Read failed: %v", err)
		}

		if outputFile != "" {
			if err := os.WriteFile(outputFile, readData, 0644); err != nil {
				log.Fatalf("Failed to write output file: %v", err)
			}
			log.Printf("Wrote %d bytes to %s", len(readData), outputFile)
		} else {
			log.Printf("Read data: '%s'", string(readData))
		}
	}

	log.Println("Done")
}

func performWrite(primary csstructs.ReplicaIdentifier, replicas []csstructs.ReplicaIdentifier, chunkHandle string, data []byte, offset int64) error {
	log.Printf("Connecting to primary at %s:%d", primary.Hostname, primary.DataPort)
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", primary.Hostname, primary.DataPort))
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()
	log.Println("Connected to primary")

	// Send action type (Download = write to chunkserver)
	actionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(actionBytes, uint32(csstructs.Download))
	if _, err = conn.Write(actionBytes); err != nil {
		return fmt.Errorf("failed to send action: %w", err)
	}

	if offset >= 0 {
		log.Printf("Random write to chunk=%s at offset=%d, size=%d bytes", chunkHandle, offset, len(data))
	} else {
		log.Printf("Append to chunk=%s, size=%d bytes", chunkHandle, len(data))
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
	log.Println("Sending authentication token")
	tokenLen := int32(len(tokenString))
	if err = binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
		return fmt.Errorf("failed to send token length: %w", err)
	}

	if _, err = conn.Write([]byte(tokenString)); err != nil {
		return fmt.Errorf("failed to send token: %w", err)
	}

	// Wait for offset allocation
	log.Println("Waiting for offset allocation from primary")
	offsetBytes := make([]byte, 8)
	if _, err = conn.Read(offsetBytes); err != nil {
		return fmt.Errorf("failed to receive offset: %w", err)
	}
	allocatedOffset := binary.BigEndian.Uint64(offsetBytes)
	log.Printf("Allocated offset: %d", allocatedOffset)

	// Send data payload
	log.Printf("Sending data payload: %d bytes", len(data))
	if _, err = conn.Write(data); err != nil {
		return fmt.Errorf("failed to send data: %w", err)
	}

	// Close write side to signal EOF
	log.Println("Closing write stream, waiting for 2PC commit")
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.CloseWrite()
	}

	// Wait for final commit response
	resultBytes := make([]byte, 1)
	if _, err = conn.Read(resultBytes); err != nil {
		return fmt.Errorf("failed to receive commit response: %w", err)
	}

	if resultBytes[0] == 1 {
		log.Printf("SUCCESS: Data persisted to chunk %s at offset %d", chunkHandle, allocatedOffset)
		return nil
	}

	return fmt.Errorf("data persistence failed")
}

func performRead(primary csstructs.ReplicaIdentifier, chunkHandle string) ([]byte, error) {
	log.Printf("Connecting to %s:%d", primary.Hostname, primary.DataPort)
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", primary.Hostname, primary.DataPort))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()
	log.Println("Connected to chunkserver")

	// Send Upload action (read from chunkserver)
	actionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(actionBytes, uint32(csstructs.Upload))
	if _, err = conn.Write(actionBytes); err != nil {
		return nil, fmt.Errorf("failed to send action: %w", err)
	}

	log.Printf("Reading chunk=%s", chunkHandle)

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
	log.Println("Sending authentication token")
	tokenLen := int32(len(tokenString))
	if err = binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
		return nil, fmt.Errorf("failed to send token length: %w", err)
	}
	if _, err = conn.Write([]byte(tokenString)); err != nil {
		return nil, fmt.Errorf("failed to send token: %w", err)
	}

	// Read response status
	log.Println("Waiting for read response")
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
	log.Printf("Chunk size: %d bytes", fileSize)

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

	log.Printf("SUCCESS: Read %d bytes from chunk %s", totalRead, chunkHandle)
	return data[:totalRead], nil
}
