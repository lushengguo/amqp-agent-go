package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bcicen/jstream"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

var logger *logrus.Logger

type Config struct {
	Server struct {
		Port int    `yaml:"port"`
		Host string `yaml:"host"`
	} `yaml:"server"`
	Queue struct {
		MaxSize string `yaml:"max_size"`
	} `yaml:"queue"`
	Log struct {
		Level        string `yaml:"level"`
		FilePath     string `yaml:"file_path"`
		MaxAge       int    `yaml:"max_age"`
		RotationTime int    `yaml:"rotation_time"`
	} `yaml:"log"`
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

func initLogger(config *Config) error {
	logger = logrus.New()

	level, err := logrus.ParseLevel(config.Log.Level)
	if err != nil {
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)

	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	logDir := filepath.Dir(config.Log.FilePath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %v", err)
	}

	maxAge := 7 * 24 * time.Hour
	rotationTime := 24 * time.Hour

	if config.Log.MaxAge > 0 {
		maxAge = time.Duration(config.Log.MaxAge) * 24 * time.Hour
	}

	if config.Log.RotationTime > 0 {
		rotationTime = time.Duration(config.Log.RotationTime) * time.Hour
	}

	writer, err := rotatelogs.New(
		config.Log.FilePath+".%Y%m%d",
		rotatelogs.WithMaxAge(maxAge),
		rotatelogs.WithRotationTime(rotationTime),
	)
	if err != nil {
		return fmt.Errorf("failed to configure log rotation: %v", err)
	}

	mw := io.MultiWriter(os.Stdout, writer)
	logger.SetOutput(mw)

	return nil
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
	data, err := os.ReadFile("config/amqp-agent.yaml")
	if err != nil {
		return nil, fmt.Errorf("error reading configuration file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("error parsing configuration file: %v", err)
	}

	if config.Log.Level == "" {
		config.Log.Level = "info"
	}
	if config.Log.FilePath == "" {
		config.Log.FilePath = "logs/amqp-agent.log"
	}
	if config.Log.MaxAge == 0 {
		config.Log.MaxAge = 7
	}
	if config.Log.RotationTime == 0 {
		config.Log.RotationTime = 24
	}

	return &config, nil
}

type AMQPConnection struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	mu   sync.Mutex
}

type ConnectionManager struct {
	connections map[string]*AMQPConnection
	mu          sync.RWMutex
}

func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections: make(map[string]*AMQPConnection),
	}
}

func (cm *ConnectionManager) GetConnection(url string, msg Message) (*amqp.Channel, error) {
	cm.mu.RLock()
	amqpConn, exists := cm.connections[url]
	cm.mu.RUnlock()

	if exists {
		amqpConn.mu.Lock()
		if amqpConn.conn != nil && !amqpConn.conn.IsClosed() {
			if amqpConn.ch != nil && !amqpConn.ch.IsClosed() {
				ch := amqpConn.ch
				amqpConn.mu.Unlock()
				return ch, nil
			}
			ch, err := amqpConn.conn.Channel()
			if err != nil {
				amqpConn.mu.Unlock()
				cm.CloseConnection(url)
				return nil, err
			}
			amqpConn.ch = ch
			amqpConn.mu.Unlock()

			logger.Debugf("Declaring exchange: %s of type: %s", msg.Exchange, msg.ExchangeType)
			err = ch.ExchangeDeclare(
				msg.Exchange,
				msg.ExchangeType,
				true,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				logger.Errorf("Error declaring exchange: %v", err)
				return nil, fmt.Errorf("error declaring exchange: %v", err)
			}

			return ch, nil
		}
		amqpConn.mu.Unlock()
		cm.CloseConnection(url)
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	amqpConn = &AMQPConnection{
		conn: conn,
		ch:   ch,
	}

	cm.mu.Lock()
	cm.connections[url] = amqpConn
	cm.mu.Unlock()

	logger.Debugf("Declaring exchange: %s of type: %s", msg.Exchange, msg.ExchangeType)
	err = ch.ExchangeDeclare(
		msg.Exchange,
		msg.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Errorf("Error declaring exchange: %v", err)
		return nil, fmt.Errorf("error declaring exchange: %v", err)
	}

	return ch, nil
}

func (cm *ConnectionManager) CloseConnection(url string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if amqpConn, exists := cm.connections[url]; exists {
		amqpConn.mu.Lock()
		if amqpConn.ch != nil {
			amqpConn.ch.Close()
		}
		if amqpConn.conn != nil {
			amqpConn.conn.Close()
		}
		amqpConn.mu.Unlock()
		delete(cm.connections, url)
	}
}

func publishMessage(ch *amqp.Channel, msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger.Debugf("Enabling confirm mode for channel...")
	if err := ch.Confirm(false); err != nil {
		logger.Errorf("Failed to enable confirm mode: %v", err)
		return fmt.Errorf("failed to enable confirm mode: %v", err)
	}

	logger.Debugf("Setting up confirm and error channels...")
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	errors := ch.NotifyClose(make(chan *amqp.Error, 1))

	logger.Debugf("Publishing message to exchange: %s with routing key: %s", msg.Exchange, msg.RoutingKey)
	err := ch.PublishWithContext(
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
		logger.Errorf("Error publishing message: %v", err)
		return fmt.Errorf("error publishing message: %v", err)
	}

	logger.Debugf("Waiting for confirmation or error...")
	select {
	case confirm := <-confirms:
		if !confirm.Ack {
			logger.Errorf("Message not acknowledged by server")
			return fmt.Errorf("message not acknowledged by server")
		}
		logger.Debugf("Message acknowledged by server")
		return nil
	case err := <-errors:
		logger.Errorf("Channel closed: %v", err)
		return fmt.Errorf("channel closed: %v", err)
	case <-ctx.Done():
		logger.Errorf("Confirmation timeout exceeded")
		return fmt.Errorf("confirmation timeout exceeded")
	}
}

func statsWorker(stats *Stats) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		received, success, failed := stats.GetStats()
		stats.Reset()
		logger.Infof("Statistics - Within 20 seconds: Received messages: %d, Successfully sent: %d, Failed: %d, Memory usage: %s",
			received, success, failed, getMemoryStats())
	}
}

func handleConnection(conn net.Conn, retryQueue *RetryQueue, stats *Stats, connManager *ConnectionManager) {
	defer conn.Close()

	decoder := jstream.NewDecoder(conn, 0)

	for streamObj := range decoder.Stream() {
		jsonData, err := json.Marshal(streamObj.Value)
		if err != nil {
			logger.Errorf("error re-marshaling JSON: %v", err)
			continue
		}

		var msg Message
		if err := json.Unmarshal(jsonData, &msg); err != nil {
			logger.Errorf("error parsing JSON: %v, data: %s", err, string(jsonData))
			continue
		}

		logger.Debugf("Received message: %+v", msg)

		stats.IncrementReceived()

		go func(msg Message) {
		ch, err := connManager.GetConnection(msg.URL, msg)
		if err != nil {
			logger.Errorf("error getting AMQP channel: %v", err)
			if err := retryQueue.Push(msg); err != nil {
				logger.Errorf("failed to add message to retry queue: %v", err)
			}
			stats.IncrementFailed()
			return
		}

		if err := publishMessage(ch, msg); err != nil {
			logger.Errorf("error publishing message: %v", err)
			if err := retryQueue.Push(msg); err != nil {
				logger.Errorf("failed to add message to retry queue: %v", err)
			}
			stats.IncrementFailed()
			connManager.CloseConnection(msg.URL)
		} else {
				logger.Debugf("message sent and acknowledged: %s", msg.Message)
				stats.IncrementSuccess()
			}
		}(msg)
	}
}

func retryWorker(retryQueue *RetryQueue, stats *Stats, connManager *ConnectionManager) {
	for {
		if retryQueue.IsEmpty() {
			time.Sleep(time.Second)
			continue
		}

		msg, ok := retryQueue.Pop()
		if !ok {
			continue
		}

		ch, err := connManager.GetConnection(msg.URL, msg)
		if err != nil {
			logger.Errorf("error retrying connection to RabbitMQ: %v", err)
			retryQueue.Push(msg)
			stats.IncrementFailed()
			time.Sleep(time.Second * 5)
			continue
		}

		if err := publishMessage(ch, msg); err != nil {
			logger.Errorf("error retrying message publishing: %v", err)
			retryQueue.Push(msg)
			stats.IncrementFailed()
			connManager.CloseConnection(msg.URL)
		} else {
			logger.Debugf("retry message sent and acknowledged: %s", msg.Message)
			stats.IncrementSuccess()
		}

		time.Sleep(time.Second)
	}
}

func main() {
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	if err := initLogger(config); err != nil {
		fmt.Printf("Error initializing logger: %v\n", err)
		os.Exit(1)
	}

	logger.Info("Starting AMQP Agent")

	retryQueue, err := NewRetryQueue(config.Queue.MaxSize)
	if err != nil {
		logger.Fatalf("Error creating retry queue: %v", err)
	}

	connManager := NewConnectionManager()
	stats := &Stats{}
	go statsWorker(stats)
	go retryWorker(retryQueue, stats, connManager)

	addr := fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatalf("Error starting server: %v", err)
	}
	defer listener.Close()

	logger.Infof("Server started at %s", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Errorf("Error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn, retryQueue, stats, connManager)
	}
}
