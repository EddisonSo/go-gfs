package chunkserver

import (
	"os"
	"net"
	"fmt"
)

type FileDownloadService struct {
	addr string
	rootDir string
	timeout int
}

func NewFileDownloadService(address, rootDir string, timeout int) *FileDownloadService {
	return &FileDownloadService{
		addr: address,
		rootDir: rootDir,
		timeout: timeout,
	}
}

func handle(conn net.Conn) {
	defer conn.Close()
	fmt.Println("new connection from", conn.RemoteAddr())
	// Handle the connection here, e.g., read the request, send file, etc.
	// For now, just close the connection
}

func (fds *FileDownloadService) DownloadFile() error {
	return nil
}

func (fds *FileDownloadService) ListenAndServe() error {
	if err := os.MkdirAll(fds.rootDir, 0o755); err != nil {
		panic(err)
	}

	ln, err := net.Listen("tcp", fds.addr)
	if err != nil {
		panic(err)
	}

	fmt.Println("listening on", fds.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
		    panic(err)
		}
		go handle(conn)
	}


	return nil
}

