package main

import (
	"encoding/json"
	"fmt"
	"sync"
)

type RetryQueue struct {
	messages    []*Message
	mu          sync.Mutex
	maxSize     uint64
	currentSize uint64
}

func NewRetryQueue(maxSize uint64) *RetryQueue {
	return &RetryQueue{
		messages:    make([]*Message, 0),
		maxSize:     maxSize,
		currentSize: 0,
	}
}

func (q *RetryQueue) Push(m *Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	newMsgBytes, _ := json.Marshal(m)
	newSize := uint64(len(newMsgBytes))
	if newSize > q.maxSize {
		return fmt.Errorf("m size (%d bytes) exceeds queue maximum limit (%d bytes)", newSize, q.maxSize)
	}

	q.messages = append(q.messages, m)
	q.currentSize += newSize
	for {
		if q.currentSize <= q.maxSize || len(q.messages) == 0 {
			break
		}

		removedMsg := q.messages[0]
		removedBytes, _ := json.Marshal(removedMsg)
		q.currentSize -= uint64(len(removedBytes))
		q.messages = q.messages[1:]

	}

	return nil
}

func (q *RetryQueue) Pop() *Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}

	m := q.messages[0]
	mBytes, _ := json.Marshal(m)
	q.currentSize -= uint64(len(mBytes))
	q.messages = q.messages[1:]
	return m
}

func (q *RetryQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages) == 0
}
