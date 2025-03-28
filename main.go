package main

import (
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
	lastReceivedCount uint64
	lastSuccessCount  uint64
	mu                sync.Mutex
}

func (s *Stats) IncrementReceived() {
	atomic.AddUint64(&s.receivedCount, 1)
}

func (s *Stats) IncrementSuccess() {
	atomic.AddUint64(&s.successCount, 1)
}

func (s *Stats) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReceivedCount = s.receivedCount
	s.lastSuccessCount = s.successCount
}

func (s *Stats) GetStats() (uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receivedCount - s.lastReceivedCount, s.successCount - s.lastSuccessCount
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
		received, success := stats.GetStats()
		stats.Reset()
		log.Printf("Statistics - Within 20 seconds: Received messages: %d, Successfully sent: %d, Memory usage: %s",
			received, success, getMemoryStats())
	}
}

func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	var multiplier int64 = 1
	var size int64
	var unit string

	_, err := fmt.Sscanf(sizeStr, "%d%s", &size, &unit)
	if err != nil {
		return 0, fmt.Errorf("Invalid size format: %v", err)
	}

	switch strings.ToUpper(unit) {
	case "KB":
		multiplier = 1024
	case "MB":
		multiplier = 1024 * 1024
	case "GB":
		multiplier = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("Unsupported unit: %s", unit)
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
		return fmt.Errorf("Message size (%d bytes) exceeds queue maximum limit (%d bytes)", newSize, q.maxSize)
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
		return nil, fmt.Errorf("Error reading configuration file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("Error parsing configuration file: %v", err)
	}

	return &config, nil
}

func publishMessage(ch *amqp.Channel, msg Message) error {

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
		return fmt.Errorf("Error declaring exchange: %v", err)
	}

	return ch.Publish(
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
}

func handleConnection(conn net.Conn, retryQueue *RetryQueue, stats *Stats) {
	defer conn.Close()

	buffer := make([]byte, 4096)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Printf("Error reading data: %v", err)
			return
		}

		var msg Message
		if err := json.Unmarshal(buffer[:n], &msg); err != nil {
			log.Printf("Error parsing JSON: %v", err)
			continue
		}

		stats.IncrementReceived()

		amqpConn, err := amqp.Dial(msg.URL)
		if err != nil {
			log.Printf("Error connecting to RabbitMQ: %v", err)
			if err := retryQueue.Push(msg); err != nil {
				log.Printf("Failed to add message to retry queue: %v", err)
			}
			continue
		}

		ch, err := amqpConn.Channel()
		if err != nil {
			amqpConn.Close()
			log.Printf("Error creating channel: %v", err)
			if err := retryQueue.Push(msg); err != nil {
				log.Printf("Failed to add message to retry queue: %v", err)
			}
			continue
		}

		if err := publishMessage(ch, msg); err != nil {
			log.Printf("Error publishing message: %v", err)
			if err := retryQueue.Push(msg); err != nil {
				log.Printf("Failed to add message to retry queue: %v", err)
			}
		} else {
			log.Printf("Message sent: %s", msg.Message)
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
			log.Printf("Error retrying connection to RabbitMQ: %v", err)
			retryQueue.Push(msg)
			time.Sleep(time.Second * 5)
			continue
		}

		ch, err := amqpConn.Channel()
		if err != nil {
			amqpConn.Close()
			log.Printf("Error retrying channel creation: %v", err)
			retryQueue.Push(msg)
			time.Sleep(time.Second * 5)
			continue
		}

		if err := publishMessage(ch, msg); err != nil {
			log.Printf("Error retrying message publishing: %v", err)
			retryQueue.Push(msg)
		} else {
			log.Printf("Retry message sent successfully: %s", msg.Message)
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
		log.Fatalf("Error loading configuration: %v", err)
	}

	retryQueue, err := NewRetryQueue(config.Queue.MaxSize)
	if err != nil {
		log.Fatalf("Error creating retry queue: %v", err)
	}

	stats := &Stats{}
	go statsWorker(stats)
	go retryWorker(retryQueue, stats)

	addr := fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	defer listener.Close()

	log.Printf("Server started at %s", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn, retryQueue, stats)
	}
}
