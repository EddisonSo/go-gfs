package transactions

import (
	"fmt"
	"sync"
)

/*
 * TransactionTracker is responsible for tracking file operation transactions in the chunk server.
 */
type TransactionTracker struct {
	transactions map[string]*Transaction 
}


var lock = &sync.Mutex{}
var TransactionTrackerInstance *TransactionTracker

func NewTransactionTracker() *TransactionTracker {
	if TransactionTrackerInstance == nil {
		lock.Lock()
		defer lock.Unlock()

		if TransactionTrackerInstance == nil {
			TransactionTrackerInstance = &TransactionTracker{make(map[string]*Transaction)}
		} else {
			fmt.Println("TransactionTracker instance already created before!")
		}
	} else {
		fmt.Println("TransactionTracker instance already created before!")
	}
	
	return TransactionTrackerInstance
}

func (tt *TransactionTracker) AddTransaction(transaction *Transaction) {
	lock.Lock()
	defer lock.Unlock()
	tt.transactions[transaction.TransactionID] = transaction
}

func (tt *TransactionTracker) GetTransaction(transactionID string) *Transaction {
	lock.Lock()
	defer lock.Unlock()
	if transaction, exists := tt.transactions[transactionID]; exists {
		return transaction
	}
	return nil
}

func (tt *TransactionTracker) RemoveTransaction(transactionID string) {
	lock.Lock()
	defer lock.Unlock()
	if _, exists := tt.transactions[transactionID]; exists {
		delete(tt.transactions, transactionID)
	} else {
		fmt.Println("Transaction with ID", transactionID, "does not exist.")
	}
}
