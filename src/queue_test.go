package main

import (
	"encoding/json"
	"strconv"
	"sync"
	"testing"
)

func TestNewRetryQueue(t *testing.T) {
	tests := []struct {
		name     string
		maxSize  uint64
		wantSize uint64
	}{
		{"创建标准队列", 1024, 1024},
		{"创建最小队列", 1, 1},
		{"创建大容量队列", 1024 * 1024, 1024 * 1024}, // 1MB
	}

	for _, testcase := range tests {
		t.Run(testcase.name, func(t *testing.T) {
			queue := NewRetryQueue(testcase.maxSize)
			if queue.maxSize != testcase.wantSize {
				t.Errorf("NewRetryQueue() maxSize = %v, want %v", queue.maxSize, testcase.wantSize)
			}
			if queue.messages == nil {
				t.Error("NewRetryQueue() messages slice should not be nil")
			}
			if len(queue.messages) != 0 {
				t.Error("NewRetryQueue() messages slice should be empty")
			}
		})
	}
}

func TestRetryQueue_DropData(t *testing.T) {
	m := Message{URL: "http://example.com", Message: "test1"}
	mBytes, _ := json.Marshal(m)
	messageSize := len(mBytes)

	q := NewRetryQueue(uint64(messageSize))
	m1 := &Message{URL: "http://example.com", Message: "test1"}
	m2 := &Message{URL: "http://example.com", Message: "test2"}

	// 添加消息
	if err := q.Push(m1); err != nil {
		t.Errorf("Push() error = %v", err)
	}
	if err := q.Push(m2); err != nil {
		t.Errorf("Push() error = %v", err)
	}

	if len(q.messages) != 1 {
		t.Errorf("Expected 1 messages in queue, got %d", len(q.messages))
	}

	message := q.Pop()
	if *message != *m2 {
		t.Errorf("Expected message %v, got %v", *m2, *message)
	}

	if len(q.messages) != 0 {
		t.Errorf("Expected 0 message in queue, got %d", len(q.messages))
	}
}

func TestRetryQueue_Push(t *testing.T) {
	m := Message{URL: "http://example.com", Message: "test1"}
	mBytes, _ := json.Marshal(m)
	messageSize := len(mBytes)

	tests := []struct {
		name      string
		queueSize uint64
		messages  []*Message
		wantErr   bool
	}{
		{
			name:      "正常推送消息",
			queueSize: uint64(messageSize),
			messages: []*Message{
				{URL: "http://example.com", Message: "test1"},
			},
			wantErr: false,
		},
		{
			name:      "队列达到容量限制时自动淘汰",
			queueSize: 2 * uint64(messageSize),
			messages: []*Message{
				{URL: "http://example.com", Message: "test1"},
				{URL: "http://example.com", Message: "test2"},
				{URL: "http://example.com", Message: "test3"},
			},
			wantErr: false,
		},
	}

	for _, testcase := range tests {
		t.Run(testcase.name, func(t *testing.T) {
			q := NewRetryQueue(testcase.queueSize)
			for i, msg := range testcase.messages {
				err := q.Push(msg)
				if (err != nil) != testcase.wantErr {
					t.Errorf("Push() message %d error = [%v], wantErr %v", i, err, testcase.wantErr)
				}
			}
		})
	}
}

func TestRetryQueue_Pop(t *testing.T) {
	q := NewRetryQueue(1024)
	messages := []*Message{
		{URL: "http://example.com", Message: "test1"},
		{URL: "http://example.com", Message: "test2"},
	}

	// 测试空队列弹出
	if msg := q.Pop(); msg != nil {
		t.Error("Pop() from empty queue should return nil")
	}

	// 添加消息并按序弹出
	for _, m := range messages {
		if err := q.Push(m); err != nil {
			t.Errorf("Push() error = %v", err)
		}
	}

	for i, want := range messages {
		got := q.Pop()
		if got == nil {
			t.Errorf("Pop() message %d = nil, want %v", i, want)
		}
		if got.Message != want.Message {
			t.Errorf("Pop() message %d = %v, want %v", i, got.Message, want.Message)
		}
	}
}

func TestRetryQueue_IsEmpty(t *testing.T) {
	q := NewRetryQueue(1024)

	if !q.IsEmpty() {
		t.Error("IsEmpty() on new queue should return true")
	}

	msg := &Message{URL: "http://example.com", Message: "test"}
	if err := q.Push(msg); err != nil {
		t.Errorf("Push() error = %v", err)
	}

	if q.IsEmpty() {
		t.Error("IsEmpty() after Push() should return false")
	}

	q.Pop()
	if !q.IsEmpty() {
		t.Error("IsEmpty() after Pop() should return true")
	}
}

func TestRetryQueue_Concurrent(t *testing.T) {
	q := NewRetryQueue(1024)
	const numGoroutines = 10
	const messagesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // 一半goroutine推送，一半弹出

	// 启动推送goroutine
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := &Message{
					URL:     "http://example.com",
					Message: strconv.Itoa(routineID*messagesPerGoroutine + j),
				}
				_ = q.Push(msg)
			}
		}(i)
	}

	// 启动弹出goroutine
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				_ = q.Pop()
			}
		}()
	}

	wg.Wait()
}
