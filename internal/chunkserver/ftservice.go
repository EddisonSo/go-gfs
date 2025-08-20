package chunkserver

import (
	"log/slog"
	"net"
	"os"
	"github.com/google/uuid"
)

type FileDownloadService struct {
	addr    string
	rootDir string
	timeout int
	transactions *TransactionTracker
}

func NewFileDownloadService(address, rootDir string, timeout int) *FileDownloadService {
	return &FileDownloadService{
		transactions: NewTransactionTracker(),
		addr:    address,
		rootDir: rootDir,
		timeout: timeout,
	}
}

func (fds *FileDownloadService) handle(conn net.Conn) {
	defer conn.Close()

	slog.Info("New connection established", "remote_addr", conn.RemoteAddr().String())

	uuidBytes := make([]byte, 16)
	n, err := conn.Read(uuidBytes)

	if err != nil {
		slog.Error("Failed to read transactionID from connection", "error", err)
		return
	}

	if n != 16 {
		slog.Error("Invalid transactionID length", "expected", 16, "got", n)
		return
	}

	transactionID, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		slog.Error("Failed to parse transactionID", "error", err)
		return
	}

	transaction := fds.transactions.GetTransaction(transactionID.String())
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

	slog.Info("FileDownloadService is listening", "address", fds.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go fds.handle(conn)
	}
}
