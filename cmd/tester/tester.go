package main

import (
	"net"
	"fmt"
	"encoding/binary"
	"eddisonso.com/go-gfs/internal/chunkserver/csstructs"
	"eddisonso.com/go-gfs/internal/chunkserver/secrets"
	"github.com/golang-jwt/jwt/v5"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}

	r1 := csstructs.ReplicaIdentifier{
		ID: "replica1",
		Hostname: "replica1.example.com",
	}

	r2 := csstructs.ReplicaIdentifier{
		ID: "replica2",
		Hostname: "replica2.example.com",
	}

	r3 := csstructs.ReplicaIdentifier{
		ID: "replica3",
		Hostname: "replica3.example.com",
	}
	
	claims := csstructs.DownloadRequestClaims{
		ChunkHandle: "1234",
		Operation: "download",
		Filesize: 5,
		Replicas: []csstructs.ReplicaIdentifier{r2, r3},
		Primary: r1,
	}
    

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	secret, err := secrets.GetSecret(nil)
	if err != nil {
		panic(err)
	}
	
	tokenString, err := token.SignedString(secret)
	fmt.Println(tokenString)
	if err != nil {
		panic(err)
	}

	num := int32(len(tokenString))
	err = binary.Write(conn, binary.BigEndian, num) // encode int32 into bytes
	if err != nil {
		panic(err)
	}

	_, err = conn.Write([]byte(tokenString))
	if err != nil {
		panic(err)
	}

	_, err = conn.Write([]byte("hello"))
}
