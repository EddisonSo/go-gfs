package chunkserver

import (
	"github.com/google/uuid"
)

const (
	UPLOAD = iota
	DOWNLOAD = iota
)

/*
 * Transaction represents a file operation transaction in the chunk server.
 * TransactionId: Unique identifier for the transaction.
 * ChunkId: Identifier for the chunk being operated on.
 * Operation: Type of operation being performed (UPLOAD or DOWNLOAD).
 */
type Transaction struct {
	TransactionID uuid.UUID
	ChunkID       string
	Operation     int
}


/*
 * Creates a new transaction with the given transaction ID, chunk ID, and operation type.
 */
func NewTransaction(chunkID string, operation int) *Transaction {
	transactionID := uuid.New()
	return &Transaction{
		TransactionID: transactionID,
		ChunkID:       chunkID,
		Operation:     operation,
	}
}

/*
 * Returns the operation type of the transaction.
 */
func (t *Transaction) GetOperation() int {
	return t.Operation
}


/*
 * Returns the transaction ID of the transaction.
 */
func (t *Transaction) GetTransactionID() uuid.UUID {
	return t.TransactionID
}

/*
 * Returns the chunk ID of the transaction.
 */
func (t *Transaction) GetChunkID() string {
	return t.ChunkID
}

