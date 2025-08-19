package chunkserver

import (
	"fmt"
	"net"
	"os"
)

type FileDownloadService struct {
	addr    string
	rootDir string
	timeout int
}

func NewFileDownloadService(address, rootDir string, timeout int) *FileDownloadService {
	return &FileDownloadService{
		addr:    address,
		rootDir: rootDir,
		timeout: timeout,
	}
}

func handle(conn net.Conn) {
	defer conn.Close()
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
}
