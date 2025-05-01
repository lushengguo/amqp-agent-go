package main

import (
	"encoding/json"
	"fmt"
	"sync"
)

type RetryQueue struct {
	messages []*Message
	mu       sync.Mutex
	maxSize  uint64
}

func NewRetryQueue(maxSize uint64) *RetryQueue {
	return &RetryQueue{
		messages: make([]*Message, 0),
		maxSize:  maxSize,
	}
}

func (q *RetryQueue) Push(m *Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	currentSize := uint64(0)
	for _, m := range q.messages {
		msgBytes, _ := json.Marshal(m)
		currentSize += uint64(len(msgBytes))
	}

	newMsgBytes, _ := json.Marshal(m)
	newSize := uint64(len(newMsgBytes))

	for currentSize+newSize > q.maxSize && len(q.messages) > 0 {
		removedMsg := q.messages[0]
		removedBytes, _ := json.Marshal(removedMsg)
		currentSize -= uint64(len(removedBytes))
		q.messages = q.messages[1:]
	}

	if newSize > q.maxSize {
		return fmt.Errorf("m size (%d bytes) exceeds queue maximum limit (%d bytes)", newSize, q.maxSize)
	}

	q.messages = append(q.messages, m)
	return nil
}

func (q *RetryQueue) Pop() *Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}

	m := q.messages[0]
	q.messages = q.messages[1:]
	return m
}

func (q *RetryQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages) == 0
}
