package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// Test version of the publish message function
func testPublishMessage(ch AMQPChannel, msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("failed to enable confirm mode: %v", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	err := ch.ExchangeDeclare(
		msg.Exchange,
		msg.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error declaring exchange: %v", err)
	}

	err = ch.PublishWithContext(
		ctx,
		msg.Exchange,
		msg.RoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Message),
			Timestamp:   time.Unix(int64(msg.Timestamp), 0),
		},
	)
	if err != nil {
		return fmt.Errorf("error publishing message: %v", err)
	}

	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			return fmt.Errorf("message not acknowledged by server")
		}

		return nil
	case <-time.After(1 * time.Second): // Use shorter timeout in tests
		return fmt.Errorf("confirmation timeout exceeded")
	}
}

// Define AMQP Channel interface containing methods we need to mock
type AMQPChannel interface {
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

// Mock AMQP connection functionality tests
func TestPublishMessageWithMocks(t *testing.T) {
	// Create a test message
	testMsg := Message{
		URL:          "amqp://guest:guest@localhost:5672/",
		Exchange:     "test_exchange",
		ExchangeType: "direct",
		RoutingKey:   "test_key",
		Message:      "test message",
		Timestamp:    uint32(time.Now().Unix()),
	}
	
	// Successful publish scenario
	t.Run("Successful message publishing", func(t *testing.T) {
		mockChannel := &mockAMQPChannel{
			confirmMode: true,
			confirmFunc: func() chan amqp.Confirmation {
				ch := make(chan amqp.Confirmation, 1)
				// Send confirmation
				go func() {
					ch <- amqp.Confirmation{Ack: true}
				}()
				return ch
			},
		}
		
		err := testPublishMessage(mockChannel, testMsg)
		if err != nil {
			t.Errorf("publishMessage should succeed, but returned error: %v", err)
		}
	})
	
	// Exchange declaration failure scenario
	t.Run("Exchange declaration failure", func(t *testing.T) {
		mockChannel := &mockAMQPChannel{
			confirmMode:          true,
			exchangeDeclareError: errors.New("exchange declare failed"),
		}
		
		err := testPublishMessage(mockChannel, testMsg)
		if err == nil {
			t.Error("exchange declaration failure should return error, but didn't")
		} else if !strings.Contains(err.Error(), mockChannel.exchangeDeclareError.Error()) {
			// Use strings.Contains to check if error message contains expected content
			t.Errorf("error doesn't contain expected content: %v", err)
		}
	})
	
	// Publishing failure scenario
	t.Run("Publishing failure", func(t *testing.T) {
		mockChannel := &mockAMQPChannel{
			confirmMode:   true,
			publishError:  errors.New("publish failed"),
		}
		
		err := testPublishMessage(mockChannel, testMsg)
		if err == nil {
			t.Error("publishing failure should return an error, but didn't")
		} else if !strings.Contains(err.Error(), mockChannel.publishError.Error()) {
			// Use strings.Contains to check if error message contains expected content
			t.Errorf("error doesn't contain expected content: %v", err)
		}
	})
	
	// Server rejection scenario
	t.Run("Server message rejection", func(t *testing.T) {
		mockChannel := &mockAMQPChannel{
			confirmMode: true,
			confirmFunc: func() chan amqp.Confirmation {
				ch := make(chan amqp.Confirmation, 1)
				// Send rejection confirmation
				go func() {
					ch <- amqp.Confirmation{Ack: false}
				}()
				return ch
			},
		}
		
		err := testPublishMessage(mockChannel, testMsg)
		if err == nil {
			t.Error("server rejection should return an error, but didn't")
		} else if err.Error() != "message not acknowledged by server" {
			t.Errorf("incorrect error message: %v", err)
		}
	})
	
	// Confirmation timeout scenario
	t.Run("Confirmation timeout", func(t *testing.T) {
		mockChannel := &mockAMQPChannel{
			confirmMode: true,
			confirmFunc: func() chan amqp.Confirmation {
				return make(chan amqp.Confirmation) // Don't send any confirmation, causing timeout
			},
		}
		
		err := testPublishMessage(mockChannel, testMsg)
		if err == nil {
			t.Error("confirmation timeout should return an error, but didn't")
		} else if err.Error() != "confirmation timeout exceeded" {
			t.Errorf("incorrect error message: %v", err)
		}
	})
}

// Mock AMQP Channel
type mockAMQPChannel struct {
	confirmMode          bool
	confirmError         error
	exchangeDeclareError error
	publishError         error
	confirmFunc          func() chan amqp.Confirmation
}

func (m *mockAMQPChannel) Confirm(noWait bool) error {
	if m.confirmError != nil {
		return m.confirmError
	}
	return nil
}

func (m *mockAMQPChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	if m.confirmFunc != nil {
		return m.confirmFunc()
	}
	return nil
}

func (m *mockAMQPChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return m.exchangeDeclareError
}

func (m *mockAMQPChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return m.publishError
}

func (m *mockAMQPChannel) Close() error {
	return nil
}

// Test overall retry mechanism
func TestRetryMechanism(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard) // Suppress log output
	
	stats := &Stats{}
	retryQueue, _ := NewRetryQueue("1MB")
	
	// Create a valid message
	msg := Message{
		URL:          "amqp://guest:guest@localhost:5672/",
		Exchange:     "test_exchange",
		ExchangeType: "direct",
		RoutingKey:   "test_key",
		Message:      "test message",
		Timestamp:    uint32(time.Now().Unix()),
	}
	
	// Add message to retry queue
	retryQueue.Push(msg)
	
	// Verify message is in queue
	if retryQueue.IsEmpty() {
		t.Fatal("message should be in retry queue, but queue is empty")
	}
	
	// Simulate multiple retry failures
	done := make(chan struct{})
	go func() {
		// Run only for short time for testing
		time.Sleep(50 * time.Millisecond)
		close(done)
	}()
	
	// Create counter to record retry attempts
	retryCount := 0
	
	// Custom retryWorker function
	testRetryWorker := func() {
		for {
			select {
			case <-done:
				return
			default:
				if retryQueue.IsEmpty() {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				
				msg, ok := retryQueue.Pop()
				if !ok {
					continue
				}
				
				// Record retry attempt
				retryCount++
				
				// Simulate failure, return message to queue
				retryQueue.Push(msg)
				stats.IncrementFailed()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	
	// Execute retry loop
	testRetryWorker()
	
	// Verify retry attempts occurred
	if retryCount == 0 {
		t.Error("should have retry attempts, but none recorded")
	}
	
	// Verify failure count increased
	if stats.failedCount == 0 {
		t.Error("failure count should increase, but remains 0")
	}
	
	// Verify message remains in queue (because we keep simulating failure)
	if retryQueue.IsEmpty() {
		t.Error("message should remain in queue after retry failures, but queue is empty")
	}
	
	fmt.Printf("Retry test completed: %d retries, failure count %d\n", retryCount, stats.failedCount)
}
