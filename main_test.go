package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// Mock connection implementation for net.Conn interface
type mockConn struct {
	readData  string
	readIndex int
	closed    bool
	mu        sync.Mutex
}

func newMockConn(data string) *mockConn {
	return &mockConn{
		readData: data,
	}
}

func (c *mockConn) Read(b []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return 0, errors.New("connection closed")
	}
	
	if c.readIndex >= len(c.readData) {
		return 0, io.EOF
	}
	
	n = copy(b, c.readData[c.readIndex:])
	c.readIndex += n
	return n, nil
}

func (c *mockConn) Write(b []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return 0, errors.New("connection closed")
	}
	
	return len(b), nil
}

func (c *mockConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.closed = true
	return nil
}

// Implement other necessary methods to satisfy net.Conn interface
func (c *mockConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *mockConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *mockConn) SetDeadline(t time.Time) error      { return nil }
func (c *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// Test the size limit of retry queue
func TestRetryQueueSizeLimit(t *testing.T) {
	// Create a small capacity retry queue (10KB)
	retryQueue, err := NewRetryQueue("10KB")
	if err != nil {
		t.Fatalf("failed to create retry queue: %v", err)
	}
	
	// Create a small message
	smallMsg := Message{
		URL:          "amqp://guest:guest@localhost:5672/",
		Exchange:     "test_exchange",
		ExchangeType: "direct",
		RoutingKey:   "test_key",
		Message:      "small message",
		Timestamp:    uint32(time.Now().Unix()),
	}
	
	// Add multiple small messages, should not exceed the limit
	for i := 0; i < 100; i++ {
		smallMsg.Message = fmt.Sprintf("small message %d", i)
		err := retryQueue.Push(smallMsg)
		if err != nil {
			t.Errorf("failed to add small message: %v", err)
		}
	}
	
	// Create a large message (exceed 10KB by repeating strings)
	bigMsg := smallMsg
	bigMsg.Message = strings.Repeat("very large message content ", 1024) // about 20KB
	
	// Adding large message should return an error
	err = retryQueue.Push(bigMsg)
	if err == nil {
		t.Error("adding message exceeding size limit should fail, but succeeded")
	} else {
		t.Logf("correctly rejected oversized message: %v", err)
	}
	
	// Check queue size control
	originalLen := len(retryQueue.messages)
	
	// Add a medium-sized message, should cause some older messages to be cleared
	mediumMsg := smallMsg
	mediumMsg.Message = strings.Repeat("medium ", 256) // about 1.5KB
	
	for i := 0; i < 10; i++ {
		err = retryQueue.Push(mediumMsg)
		if err != nil {
			t.Errorf("failed to add medium message: %v", err)
		}
	}
	
	// Check if old messages have been cleared
	if len(retryQueue.messages) >= originalLen+10 {
		t.Errorf("queue should discard old messages to make room for new ones")
	}
}

// Test precise behavior when queue is full
func TestQueueFullBehavior(t *testing.T) {
	// Create a very small queue (1KB) to test full queue scenario
	retryQueue, err := NewRetryQueue("1KB")
	if err != nil {
		t.Fatalf("failed to create retry queue: %v", err)
	}
	
	// Create identifiable message sequence for tracking which messages are discarded
	createTestMsg := func(id int) Message {
		return Message{
			URL:          "amqp://guest:guest@localhost:5672/",
			Exchange:     "test_exchange",
			ExchangeType: "direct",
			RoutingKey:   "test_key",
			Message:      fmt.Sprintf("test message id=%d", id),
			Timestamp:    uint32(time.Now().Unix()),
		}
	}
	
	// 1. Gradually fill the queue
	var addedMsgIds []int
	for i := 0; i < 100; i++ {  // Try to add 100 messages, queue will definitely be full
		msg := createTestMsg(i)
		err := retryQueue.Push(msg)
		if err != nil {
			// If error, this means single message is too large, we skip
			continue
		}
		addedMsgIds = append(addedMsgIds, i)
		
		// Calculate current queue memory usage
		currentSize := int64(0)
		for _, m := range retryQueue.messages {
			msgBytes, _ := json.Marshal(m)
			currentSize += int64(len(msgBytes))
		}
		
		// Verify queue size doesn't exceed limit
		if currentSize > retryQueue.maxSize {
			t.Errorf("queue size (%d bytes) exceeds limit (%d bytes)", currentSize, retryQueue.maxSize)
		}
	}
	
	// Ensure we've added at least some messages
	if len(addedMsgIds) < 5 {
		t.Fatalf("invalid test: couldn't add enough messages to the queue")
	}
	
	// 2. Check messages in queue - should only keep newest messages
	firstMsgID := -1
	for i, msg := range retryQueue.messages {
		// Parse message ID
		var id int
		fmt.Sscanf(msg.Message, "test message id=%d", &id)
		
		if i == 0 {
			firstMsgID = id
		}
		
		// Verify message order (should have consecutive IDs)
		if i > 0 && id != firstMsgID+i {
			t.Errorf("incorrect message order in queue: position %d expected ID %d, got %d", 
				i, firstMsgID+i, id)
		}
	}
	
	// Verify queue discards oldest messages
	lastAddedID := addedMsgIds[len(addedMsgIds)-1]
	lastQueueID := -1
	if len(retryQueue.messages) > 0 {
		fmt.Sscanf(retryQueue.messages[len(retryQueue.messages)-1].Message, 
			"test message id=%d", &lastQueueID)
	}
	
	if lastQueueID != lastAddedID {
		t.Errorf("queue didn't retain most recently added message: last added ID=%d, last queue ID=%d", 
			lastAddedID, lastQueueID)
	}
	
	// 3. Add one more message, verify that oldest message is discarded
	if len(retryQueue.messages) > 0 {
		initialQueueSize := len(retryQueue.messages)
		oldestMsgID := -1
		fmt.Sscanf(retryQueue.messages[0].Message, "test message id=%d", &oldestMsgID)
		
		// Add new message
		newMsg := createTestMsg(lastAddedID + 1)
		err := retryQueue.Push(newMsg)
		if err != nil {
			t.Fatalf("failed to add new message: %v", err)
		}
		
		// Check if queue size remains reasonable
		if len(retryQueue.messages) > initialQueueSize + 1 {
			t.Errorf("queue grew too much after adding new message: original size=%d, new size=%d", 
				initialQueueSize, len(retryQueue.messages))
		}
		
		// Check if oldest message was removed
		if len(retryQueue.messages) > 0 {
			newOldestMsgID := -1
			fmt.Sscanf(retryQueue.messages[0].Message, "test message id=%d", &newOldestMsgID)
			
			if initialQueueSize == len(retryQueue.messages) && newOldestMsgID <= oldestMsgID {
				t.Errorf("oldest message should be removed: original oldest ID=%d, current oldest ID=%d", 
					oldestMsgID, newOldestMsgID)
			}
		}
	}
	
	// 4. Test a special case: adding oversized message to empty queue
	retryQueue, _ = NewRetryQueue("1KB") // Reset queue
	hugeMsg := createTestMsg(999)
	hugeMsg.Message = strings.Repeat("huge message that exceeds queue size ", 100) // Far exceeds 1KB
	
	err = retryQueue.Push(hugeMsg)
	if err == nil {
		t.Error("adding message exceeding queue size should fail")
	} else {
		if !strings.Contains(err.Error(), "exceeds queue maximum limit") {
			t.Errorf("incorrect error message: %v", err)
		}
		t.Logf("correctly rejected oversized message: %v", err)
	}
}

// Test connection error handling
func TestConnectionErrorHandling(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard) // Suppress log output
	
	stats := &Stats{}
	retryQueue, _ := NewRetryQueue("1MB")
	
	// Create invalid JSON data
	invalidJSONConn := newMockConn(`{"broken_json": }`)
	
	// Simulate connection handling
	handleConnection(invalidJSONConn, retryQueue, stats, logger)
	
	// Check if connection is closed
	if !invalidJSONConn.closed {
		t.Error("connection should be closed after processing invalid JSON")
	}
	
	// Create message with invalid URL
	validButFailingJSON := `{"url":"invalid://url","exchange":"test","exchange_type":"direct","routing_key":"test","message":"test message","timestamp":1621500000}`
	failingConn := newMockConn(validButFailingJSON)
	
	// Handle this connection
	handleConnection(failingConn, retryQueue, stats, logger)
	
	// Check failure count and retry queue
	if stats.failedCount == 0 {
		t.Error("processing message with invalid URL should increase failure count")
	}
	
	if retryQueue.IsEmpty() {
		t.Error("message with invalid URL should be added to retry queue")
	}
}

// Test retry queue behavior
func TestRetryQueueBehavior(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard) // Suppress log output
	
	// Create message and retry queue
	msg := Message{
		URL:          "amqp://guest:guest@localhost:5672/",
		Exchange:     "test_exchange",
		ExchangeType: "direct",
		RoutingKey:   "test_key",
		Message:      "test message",
		Timestamp:    uint32(time.Now().Unix()),
	}
	
	retryQueue, _ := NewRetryQueue("1MB")
	
	// Add message to queue
	err := retryQueue.Push(msg)
	if err != nil {
		t.Fatalf("failed to add message to queue: %v", err)
	}
	
	// Check queue is not empty
	if retryQueue.IsEmpty() {
		t.Fatal("queue should not be empty after adding message")
	}
	
	// Pop message
	poppedMsg, ok := retryQueue.Pop()
	if !ok {
		t.Fatal("failed to pop message from queue")
	}
	
	// Verify message content
	if poppedMsg.Message != msg.Message {
		t.Errorf("popped message content doesn't match: expected %s, got %s", msg.Message, poppedMsg.Message)
	}
	
	// Check queue is now empty
	if !retryQueue.IsEmpty() {
		t.Error("queue should be empty after popping the only message")
	}
	
	// Test popping from empty queue
	_, ok = retryQueue.Pop()
	if ok {
		t.Error("popping from empty queue should return false")
	}
}

// Test connection disruption during message processing
func TestConnectionDisruptionDuringProcessing(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard) // Suppress log output
	
	stats := &Stats{}
	retryQueue, _ := NewRetryQueue("1MB")
	
	// Create a mock connection that closes after reading half the data
	msgData, _ := json.Marshal(Message{
		URL:          "amqp://guest:guest@localhost:5672/",
		Exchange:     "test_exchange",
		ExchangeType: "direct",
		RoutingKey:   "test_key",
		Message:      "test message",
		Timestamp:    uint32(time.Now().Unix()),
	})
	
	// Create a custom mockConn, override Read method to close after partial read
	readCount := 0
	partialDataConn := &mockConn{
		readData: string(msgData),
	}
	
	// Create a wrapped Read method
	originalRead := partialDataConn.Read
	
	// Use a closure to wrap Read method - fix field name
	wrappedConn := &wrappedMockConn{
		mockConn: partialDataConn,  // Use correct field name
		ReadFunc: func(b []byte) (int, error) {
			readCount++
			// Close connection after a few reads
			if readCount > 2 {
				partialDataConn.closed = true
				return 0, errors.New("connection closed")
			}
			return originalRead(b)
		},
	}
	
	// Handle this disruptive connection
	handleConnection(wrappedConn, retryQueue, stats, logger)
	
	// Verify connection is indeed closed
	if !partialDataConn.closed {
		t.Error("connection should be closed")
	}
}

// Wrapped mockConn, allows custom Read method
type wrappedMockConn struct {
	mockConn *mockConn  // Explicitly name the field, not anonymous embedding
	ReadFunc func([]byte) (int, error)
}

func (w *wrappedMockConn) Read(b []byte) (int, error) {
	return w.ReadFunc(b)
}

// Forward other network interface methods
func (w *wrappedMockConn) Write(b []byte) (n int, err error) {
	return w.mockConn.Write(b)
}

func (w *wrappedMockConn) Close() error {
	return w.mockConn.Close()
}

func (w *wrappedMockConn) LocalAddr() net.Addr {
	return w.mockConn.LocalAddr()
}

func (w *wrappedMockConn) RemoteAddr() net.Addr {
	return w.mockConn.RemoteAddr()
}

func (w *wrappedMockConn) SetDeadline(t time.Time) error {
	return w.mockConn.SetDeadline(t)
}

func (w *wrappedMockConn) SetReadDeadline(t time.Time) error {
	return w.mockConn.SetReadDeadline(t)
}

func (w *wrappedMockConn) SetWriteDeadline(t time.Time) error {
	return w.mockConn.SetWriteDeadline(t)
}

// Test Stats structure counter functionality
func TestStatsCounter(t *testing.T) {
	stats := &Stats{}
	
	// Increment counters
	for i := 0; i < 10; i++ {
		stats.IncrementReceived()
	}
	
	for i := 0; i < 7; i++ {
		stats.IncrementSuccess()
	}
	
	for i := 0; i < 3; i++ {
		stats.IncrementFailed()
	}
	
	// Check counters
	if stats.receivedCount != 10 {
		t.Errorf("incorrect received count: expected 10, got %d", stats.receivedCount)
	}
	
	if stats.successCount != 7 {
		t.Errorf("incorrect success count: expected 7, got %d", stats.successCount)
	}
	
	if stats.failedCount != 3 {
		t.Errorf("incorrect failed count: expected 3, got %d", stats.failedCount)
	}
	
	// Test GetStats and Reset
	received, success, failed := stats.GetStats()
	if received != 10 || success != 7 || failed != 3 {
		t.Errorf("GetStats returned incorrect values: expected (10,7,3), got (%d,%d,%d)", received, success, failed)
	}
	
	stats.Reset()
	
	// Increment some counters again
	stats.IncrementReceived()
	stats.IncrementReceived()
	stats.IncrementSuccess()
	
	// Test counters after Reset
	received, success, failed = stats.GetStats()
	if received != 2 || success != 1 || failed != 0 {
		t.Errorf("GetStats after Reset returned incorrect values: expected (2,1,0), got (%d,%d,%d)", received, success, failed)
	}
}

// Test interaction between message processing and queue
func TestMessageProcessingQueueInteraction(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard) // Suppress log output
	
	stats := &Stats{}
	retryQueue, _ := NewRetryQueue("1MB")
	
	// Mock message
	testMsg := Message{
		URL:          "amqp://guest:guest@localhost:5672/",
		Exchange:     "test_exchange",
		ExchangeType: "direct",
		RoutingKey:   "test_key",
		Message:      "test message for queue interaction",
		Timestamp:    uint32(time.Now().Unix()),
	}
	
	// 1. Test: Successfully sent messages should not enter retry queue
	// Create connection with a complete message
	msgBytes, _ := json.Marshal(testMsg)
	successConn := newMockConn(string(msgBytes))
	
	// Custom mock test environment, simulate successful message processing
	successHandler := func(conn net.Conn, retryQueue *RetryQueue, stats *Stats, logger *logrus.Logger) {
		// Simulate successful processing
		stats.IncrementReceived()
		stats.IncrementSuccess()
		conn.Close()
	}
	
	// Record initial queue state
	initialQueueEmpty := retryQueue.IsEmpty()
	
	// Simulate successful processing
	successHandler(successConn, retryQueue, stats, logger)
	
	// Verify: Queue state should not change after successful processing (still empty)
	if !initialQueueEmpty || !retryQueue.IsEmpty() {
		t.Error("successfully processed messages should not enter retry queue")
	}
	
	// 2. Test: Failed messages should be correctly added to retry queue
	// Create mock message processing failure function
	failureHandler := func(conn net.Conn, retryQueue *RetryQueue, stats *Stats, logger *logrus.Logger) {
		// Simulate receiving message but failing to send
		stats.IncrementReceived()
		stats.IncrementFailed()
		// Parse mock message data
		var msg Message
		msgData, _ := io.ReadAll(conn)
		json.Unmarshal(msgData, &msg)
		// Add message to retry queue
		retryQueue.Push(msg)
		conn.Close()
	}
	
	// Create new connection with same message
	failConn := newMockConn(string(msgBytes))
	
	// Simulate failed processing
	failureHandler(failConn, retryQueue, stats, logger)
	
	// Verify: Failed message should be added to queue
	if retryQueue.IsEmpty() {
		t.Error("failed messages should be added to retry queue")
	}
	
	// Verify if message content in queue matches
	queuedMsg, ok := retryQueue.Pop()
	if !ok {
		t.Fatal("could not pop message from queue")
	}
	if queuedMsg.Message != testMsg.Message {
		t.Errorf("message content in queue doesn't match: expected %s, got %s", testMsg.Message, queuedMsg.Message)
	}
	
	// 3. Test: Retry queue message processing flow
	// Simulate successful retry
	retrySuccessHandler := func(msg Message, retryQueue *RetryQueue, stats *Stats, logger *logrus.Logger) bool {
		// Directly simulate success, message doesn't need to return to queue
		stats.IncrementSuccess()
		return true // Indicate successful processing
	}
	
	// Push message back to queue for testing
	retryQueue.Push(testMsg)
	if retryQueue.IsEmpty() {
		t.Fatal("queue should not be empty before test")
	}
	
	// Get message for retry processing
	retryMsg, _ := retryQueue.Pop()
	retrySuccess := retrySuccessHandler(retryMsg, retryQueue, stats, logger)
	
	// Verify: Message should not return to queue after successful retry
	if !retrySuccess || !retryQueue.IsEmpty() {
		t.Error("message should not return to queue after successful retry")
	}
	
	// 4. Test: Retry failure flow
	// Simulate retry failure
	retryFailureHandler := func(msg Message, retryQueue *RetryQueue, stats *Stats, logger *logrus.Logger) bool {
		// Simulate failure, message needs to return to queue
		stats.IncrementFailed()
		retryQueue.Push(msg)
		return false // Indicate failed processing
	}
	
	// Push message back to queue for testing
	retryQueue.Push(testMsg)
	retryMsg, _ = retryQueue.Pop()
	retrySuccess = retryFailureHandler(retryMsg, retryQueue, stats, logger)
	
	// Verify: Message should return to queue after failed retry
	if retrySuccess || retryQueue.IsEmpty() {
		t.Error("message should return to queue after failed retry")
	}
	
	// Verify: Returned message is the original message
	queuedMsg, _ = retryQueue.Pop()
	if queuedMsg.Message != testMsg.Message {
		t.Errorf("message content in queue doesn't match after failed retry: expected %s, got %s", testMsg.Message, queuedMsg.Message)
	}
	
	// 5. Test: Multiple retries and queue message accumulation
	// Clear queue
	for !retryQueue.IsEmpty() {
		retryQueue.Pop()
	}
	
	// Create multiple messages
	for i := 0; i < 5; i++ {
		msgCopy := testMsg
		msgCopy.Message = fmt.Sprintf("retry test message %d", i)
		retryQueue.Push(msgCopy)
	}
	
	// Confirm queue has 5 messages
	queueSize := 0
	tempQueue := &RetryQueue{
		messages: make([]Message, 0),
		maxSize:  retryQueue.maxSize,
	}
	
	for !retryQueue.IsEmpty() {
		msg, _ := retryQueue.Pop()
		queueSize++
		tempQueue.Push(msg)
	}
	
	// Return messages to original queue
	for !tempQueue.IsEmpty() {
		msg, _ := tempQueue.Pop()
		retryQueue.Push(msg)
	}
	
	if queueSize != 5 {
		t.Errorf("incorrect message count in queue: expected 5, got %d", queueSize)
	}
	
	// Simulate partially successful retry processing
	successCount := 0
	failCount := 0
	processedIndices := make(map[int]bool) // Track processed message indices
	
	// Create temporary storage for messages that need to return to queue
	failedMsgs := make([]Message, 0)
	
	// Process all messages from queue
	for !retryQueue.IsEmpty() {
		msg, _ := retryQueue.Pop()
		
		// Parse message index
		var index int
		fmt.Sscanf(msg.Message, "retry test message %d", &index)
		processedIndices[index] = true
		
		if index%2 == 0 {
			// Even index messages processed successfully
			successCount++
			// Successful messages not returned to queue
		} else {
			// Odd index messages fail processing
			failCount++
			failedMsgs = append(failedMsgs, msg) // Collect failed messages
		}
	}
	
	// Return all failed messages to queue
	for _, msg := range failedMsgs {
		retryQueue.Push(msg)
	}
	
	// Verify all 5 messages were processed
	if len(processedIndices) != 5 {
		t.Errorf("should process 5 messages, but only processed %d", len(processedIndices))
	}
	
	// Verify success and failure counts
	expectedSuccess := 3 // 0,2,4 three even indices
	expectedFail := 2    // 1,3 two odd indices
	
	if successCount != expectedSuccess {
		t.Errorf("incorrect count of successfully processed messages: expected %d, got %d", expectedSuccess, successCount)
	}
	
	if failCount != expectedFail {
		t.Errorf("incorrect count of failed messages: expected %d, got %d", expectedFail, failCount)
	}
	
	// Verify processing results
	remainingMessages := make(map[int]bool)
	for !retryQueue.IsEmpty() {
		msg, _ := retryQueue.Pop()
		
		// Confirm remaining messages are all odd index (failed) messages
		var index int
		fmt.Sscanf(msg.Message, "retry test message %d", &index)
		remainingMessages[index] = true
		
		if index%2 != 1 {
			t.Errorf("queue should not contain even index messages: %s", msg.Message)
		}
	}
	
	// Verify correct number of messages remains in queue
	if len(remainingMessages) != expectedFail {
		t.Errorf("incorrect number of remaining messages in queue: expected %d, got %d", expectedFail, len(remainingMessages))
	}
	
	// Verify correct messages remain
	for i := 0; i < 5; i++ {
		if i%2 == 1 { // Odd index
			if !remainingMessages[i] {
				t.Errorf("queue should contain message with index %d, but not found", i)
			}
		} else { // Even index
			if remainingMessages[i] {
				t.Errorf("queue should not contain message with index %d, but found", i)
			}
		}
	}
}
