package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"

	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"github.com/golang-jwt/jwt/v5"
)

func main() {
	log.Println("Starting GFS write test")

	// Define replica topology
	primary := csstructs.ReplicaIdentifier{
		ID:              "replica1",
		Hostname:        "localhost",
		DataPort:        8080,
		ReplicationPort: 8081,
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

	// Connect to primary chunkserver
	log.Printf("Connecting to primary at %s:%d", primary.Hostname, primary.DataPort)
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", primary.Hostname, primary.DataPort))
	if err != nil {
		log.Fatalf("Failed to connect to primary: %v", err)
	}
	defer conn.Close()
	log.Println("Connected to primary")

	// Send action type
	action := csstructs.Download
	actionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(actionBytes, uint32(action))
	if _, err = conn.Write(actionBytes); err != nil {
		log.Fatalf("Failed to send action: %v", err)
	}

	// Prepare test data
	testData := "hello"
	chunkHandle := "1234"
	log.Printf("Preparing write operation: chunk=%s, data='%s', size=%d bytes", chunkHandle, testData, len(testData))

	// Create JWT with write metadata
	claims := csstructs.DownloadRequestClaims{
		ChunkHandle: chunkHandle,
		Operation:   "download",
		Filesize:    uint64(len(testData)),
		Replicas:    []csstructs.ReplicaIdentifier{r2, r3},
		Primary:     primary,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	secret, err := secrets.GetSecret(nil)
	if err != nil {
		log.Fatalf("Failed to get secret: %v", err)
	}

	tokenString, err := token.SignedString(secret)
	if err != nil {
		log.Fatalf("Failed to sign token: %v", err)
	}

	// Send JWT token
	log.Println("Sending authentication token")
	tokenLen := int32(len(tokenString))
	if err = binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
		log.Fatalf("Failed to send token length: %v", err)
	}

	if _, err = conn.Write([]byte(tokenString)); err != nil {
		log.Fatalf("Failed to send token: %v", err)
	}

	// Wait for offset allocation
	log.Println("Waiting for offset allocation from primary")
	offsetBytes := make([]byte, 8)
	if _, err = conn.Read(offsetBytes); err != nil {
		log.Fatalf("Failed to receive offset: %v", err)
	}
	offset := binary.BigEndian.Uint64(offsetBytes)
	log.Printf("Allocated offset: %d", offset)

	// Send data payload
	log.Printf("Sending data payload: %d bytes", len(testData))
	if _, err = conn.Write([]byte(testData)); err != nil {
		log.Fatalf("Failed to send data: %v", err)
	}

	// Close write side to signal EOF
	log.Println("Closing write stream, waiting for 2PC commit")
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.CloseWrite()
	}

	// Wait for final commit response
	resultBytes := make([]byte, 1)
	if _, err = conn.Read(resultBytes); err != nil {
		log.Fatalf("Failed to receive commit response: %v", err)
	}

	// Report final result
	if resultBytes[0] == 1 {
		log.Printf("SUCCESS: Data persisted to chunk %s at offset %d", chunkHandle, offset)
		log.Printf("Replicas: primary=%s, secondary=[%s, %s]", primary.ID, r2.ID, r3.ID)
	} else {
		log.Printf("FAILURE: Data persistence failed for chunk %s", chunkHandle)
	}
}
