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
	primary := csstructs.ReplicaIdentifier{
		ID: "replica1",
		Hostname: "localhost",
		DataPort: 8080,
		ReplicationPort: 8081,
	}

	r2 := csstructs.ReplicaIdentifier{
		ID: "replica2",
		Hostname: "chunkserver2",
		DataPort: 8080,
		ReplicationPort: 8081,
	}

	r3 := csstructs.ReplicaIdentifier{
		ID: "replica3",
		Hostname: "chunkserver3",
		DataPort: 8080,
		ReplicationPort: 8081,
	}

	conn, err := net.Dial("tcp", primary.Hostname + ":" + fmt.Sprint(primary.DataPort))
	if err != nil {
		panic(err)
	}

	action := csstructs.Download
	actionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(actionBytes, uint32(action))
	_, err = conn.Write(actionBytes)
	if err != nil {
		panic(err)
	}


	
	claims := csstructs.DownloadRequestClaims{
		ChunkHandle: "1234",
		Operation: "download",
		Filesize: 4,
		Replicas: []csstructs.ReplicaIdentifier{r2, r3},
		Primary: primary,
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
