package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"github.com/golang-jwt/jwt/v5"
)

var (
	doWrite     bool
	doRead      bool
	inputFile   string
	outputFile  string
	chunkHandle string
	host        string
	port        int
	data        string
)

func init() {
	flag.BoolVar(&doWrite, "write", false, "Perform write operation")
	flag.BoolVar(&doRead, "read", false, "Perform read operation")
	flag.StringVar(&inputFile, "input", "", "Input file for write operation (reads from file instead of --data)")
	flag.StringVar(&outputFile, "output", "", "Output file for read operation (writes chunk data to file)")
	flag.StringVar(&chunkHandle, "chunk", "1234", "Chunk handle to read/write")
	flag.StringVar(&host, "host", "localhost", "Chunkserver hostname")
	flag.IntVar(&port, "port", 8080, "Chunkserver data port")
	flag.StringVar(&data, "data", "hello", "Data to write (used if --input not specified)")
}

func main() {
	flag.Parse()

	// If neither read nor write specified, run both (original test behavior)
	if !doWrite && !doRead {
		doWrite = true
		doRead = true
	}

	// Define replica topology
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

	var writeData []byte

	if doWrite {
		// Get data to write
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

		log.Println("=== Write Operation ===")
		if err := performWrite(primary, replicas, chunkHandle, writeData); err != nil {
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

func performWrite(primary csstructs.ReplicaIdentifier, replicas []csstructs.ReplicaIdentifier, chunkHandle string, data []byte) error {
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

	log.Printf("Writing to chunk=%s, size=%d bytes", chunkHandle, len(data))

	// Create JWT with write metadata
	claims := csstructs.DownloadRequestClaims{
		ChunkHandle: chunkHandle,
		Operation:   "download",
		Filesize:    uint64(len(data)),
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
	offset := binary.BigEndian.Uint64(offsetBytes)
	log.Printf("Allocated offset: %d", offset)

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
		log.Printf("SUCCESS: Data persisted to chunk %s at offset %d", chunkHandle, offset)
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
