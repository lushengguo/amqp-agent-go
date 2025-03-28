package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server struct {
		Port int    `yaml:"port"`
		Host string `yaml:"host"`
	} `yaml:"server"`
	Queue struct {
		MaxSize string `yaml:"max_size"`
	} `yaml:"queue"`
}

type Message struct {
	URL          string `json:"url"`
	Exchange     string `json:"exchange"`
	ExchangeType string `json:"exchange_type"`
	RoutingKey   string `json:"routing_key"`
	Message      string `json:"message"`
	Timestamp    uint32 `json:"timestamp"`
}

type RetryQueue struct {
	messages []Message
	mu       sync.Mutex
	maxSize  int64
}

type Stats struct {
	receivedCount     uint64
	successCount      uint64
	failedCount       uint64
	lastReceivedCount uint64
	lastSuccessCount  uint64
	lastFailedCount   uint64
	mu                sync.Mutex
}

func (s *Stats) IncrementReceived() {
	atomic.AddUint64(&s.receivedCount, 1)
}

func (s *Stats) IncrementSuccess() {
	atomic.AddUint64(&s.successCount, 1)
}

func (s *Stats) IncrementFailed() {
	atomic.AddUint64(&s.failedCount, 1)
}

func (s *Stats) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReceivedCount = s.receivedCount
	s.lastSuccessCount = s.successCount
	s.lastFailedCount = s.failedCount
}

func (s *Stats) GetStats() (uint64, uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receivedCount - s.lastReceivedCount, 
	       s.successCount - s.lastSuccessCount,
	       s.failedCount - s.lastFailedCount
}

func getMemoryStats() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return fmt.Sprintf("Alloc: %v MiB, Sys: %v MiB, NumGC: %v",
		m.Alloc/1024/1024,
		m.Sys/1024/1024,
		m.NumGC)
}

func statsWorker(stats *Stats) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		received, success, failed := stats.GetStats()
		stats.Reset()
		log.Printf("Statistics - Within 20 seconds: Received messages: %d, Successfully sent: %d, Failed: %d, Memory usage: %s",
			received, success, failed, getMemoryStats())
	}
}

func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	var multiplier int64 = 1
	var size int64
	var unit string

	_, err := fmt.Sscanf(sizeStr, "%d%s", &size, &unit)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %v", err)
	}

	switch strings.ToUpper(unit) {
	case "KB":
		multiplier = 1024
	case "MB":
		multiplier = 1024 * 1024
	case "GB":
		multiplier = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unsupported unit: %s", unit)
	}

	return size * multiplier, nil
}

func NewRetryQueue(maxSize string) (*RetryQueue, error) {
	size, err := parseSize(maxSize)
	if err != nil {
		return nil, err
	}
	return &RetryQueue{
		messages: make([]Message, 0),
		maxSize:  size,
	}, nil
}

func (q *RetryQueue) Push(msg Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	currentSize := int64(0)
	for _, m := range q.messages {
		msgBytes, _ := json.Marshal(m)
		currentSize += int64(len(msgBytes))
	}

	newMsgBytes, _ := json.Marshal(msg)
	newSize := int64(len(newMsgBytes))

	for currentSize+newSize > q.maxSize && len(q.messages) > 0 {
		removedMsg := q.messages[0]
		removedBytes, _ := json.Marshal(removedMsg)
		currentSize -= int64(len(removedBytes))
		q.messages = q.messages[1:]
	}

	if newSize > q.maxSize {
		return fmt.Errorf("message size (%d bytes) exceeds queue maximum limit (%d bytes)", newSize, q.maxSize)
	}

	q.messages = append(q.messages, msg)
	return nil
}

func (q *RetryQueue) Pop() (Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return Message{}, false
	}

	msg := q.messages[0]
	q.messages = q.messages[1:]
	return msg, true
}

func (q *RetryQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages) == 0
}

func loadConfig() (*Config, error) {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		return nil, fmt.Errorf("error reading configuration file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing configuration file: %v", err)
	}

	return &config, nil
}

func publishMessage(ch *amqp.Channel, msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enable confirm mode
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("failed to enable confirm mode: %v", err)
	}

	// Create confirm channel to receive publish confirmations
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

	// Wait for confirmation with timeout
	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			return fmt.Errorf("message not acknowledged by server")
		}
		// Message was successfully confirmed
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("confirmation timeout exceeded")
	}
}

func handleConnection(conn net.Conn, retryQueue *RetryQueue, stats *Stats) {
	defer conn.Close()

	buffer := make([]byte, 4096)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Printf("error reading data: %v", err)
			return
		}

		var msg Message
		if err := json.Unmarshal(buffer[:n], &msg); err != nil {
			log.Printf("error parsing JSON: %v", err)
			continue
		}

		stats.IncrementReceived()

		amqpConn, err := amqp.Dial(msg.URL)
		if err != nil {
			log.Printf("error connecting to RabbitMQ: %v", err)
			if err := retryQueue.Push(msg); err != nil {
				log.Printf("failed to add message to retry queue: %v", err)
			}
			stats.IncrementFailed()
			continue
		}

		ch, err := amqpConn.Channel()
		if err != nil {
			amqpConn.Close()
			log.Printf("error creating channel: %v", err)
			if err := retryQueue.Push(msg); err != nil {
				log.Printf("failed to add message to retry queue: %v", err)
			}
			stats.IncrementFailed()
			continue
		}

		if err := publishMessage(ch, msg); err != nil {
			log.Printf("error publishing message: %v", err)
			if err := retryQueue.Push(msg); err != nil {
				log.Printf("failed to add message to retry queue: %v", err)
			}
			stats.IncrementFailed()
		} else {
			log.Printf("message sent and acknowledged: %s", msg.Message)
			stats.IncrementSuccess()
		}

		ch.Close()
		amqpConn.Close()
	}
}

func retryWorker(retryQueue *RetryQueue, stats *Stats) {
	for {
		if retryQueue.IsEmpty() {
			time.Sleep(time.Second)
			continue
		}

		msg, ok := retryQueue.Pop()
		if !ok {
			continue
		}

		amqpConn, err := amqp.Dial(msg.URL)
		if err != nil {
			log.Printf("error retrying connection to RabbitMQ: %v", err)
			retryQueue.Push(msg)
			stats.IncrementFailed()
			time.Sleep(time.Second * 5)
			continue
		}

		ch, err := amqpConn.Channel()
		if err != nil {
			amqpConn.Close()
			log.Printf("error retrying channel creation: %v", err)
			retryQueue.Push(msg)
			stats.IncrementFailed()
			time.Sleep(time.Second * 5)
			continue
		}

		if err := publishMessage(ch, msg); err != nil {
			log.Printf("error retrying message publishing: %v", err)
			retryQueue.Push(msg)
			stats.IncrementFailed()
		} else {
			log.Printf("retry message sent and acknowledged: %s", msg.Message)
			stats.IncrementSuccess()
		}

		ch.Close()
		amqpConn.Close()
		time.Sleep(time.Second)
	}
}

func main() {
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("error loading configuration: %v", err)
	}

	retryQueue, err := NewRetryQueue(config.Queue.MaxSize)
	if err != nil {
		log.Fatalf("error creating retry queue: %v", err)
	}

	stats := &Stats{}
	go statsWorker(stats)
	go retryWorker(retryQueue, stats)

	addr := fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("error starting server: %v", err)
	}
	defer listener.Close()

	log.Printf("server started at %s", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn, retryQueue, stats)
	}
}
