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
    receivedCount      uint64
    successCount       uint64
    lastReceivedCount  uint64
    lastSuccessCount   uint64
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
        log.Printf("统计信息 - 20秒内: 接收消息: %d, 成功发送: %d, 内存使用: %s",
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
        return 0, fmt.Errorf("无效的大小格式: %v", err)
    }

    switch strings.ToUpper(unit) {
    case "KB":
        multiplier = 1024
    case "MB":
        multiplier = 1024 * 1024
    case "GB":
        multiplier = 1024 * 1024 * 1024
    default:
        return 0, fmt.Errorf("不支持的单位: %s", unit)
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
        return fmt.Errorf("消息大小 (%d bytes) 超过队列最大限制 (%d bytes)", newSize, q.maxSize)
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
        return nil, fmt.Errorf("读取配置文件错误: %v", err)
    }

    var config Config
    if err := yaml.Unmarshal(data, &config); err != nil {
        return nil, fmt.Errorf("解析配置文件错误: %v", err)
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
        return fmt.Errorf("声明交换机错误: %v", err)
    }

    
    return ch.Publish(
        msg.Exchange,   
        msg.RoutingKey, 
        false,         
        false,         
        amqp.Publishing{
            ContentType: "text/plain",
            Body:       []byte(msg.Message),
            Timestamp:  time.Unix(int64(msg.Timestamp), 0),
        },
    )
}

func handleConnection(conn net.Conn, retryQueue *RetryQueue, stats *Stats) {
    defer conn.Close()

    buffer := make([]byte, 4096)
    for {
        n, err := conn.Read(buffer)
        if err != nil {
            log.Printf("读取数据错误: %v", err)
            return
        }

        var msg Message
        if err := json.Unmarshal(buffer[:n], &msg); err != nil {
            log.Printf("解析 JSON 错误: %v", err)
            continue
        }

        stats.IncrementReceived()

        
        amqpConn, err := amqp.Dial(msg.URL)
        if err != nil {
            log.Printf("连接 RabbitMQ 错误: %v", err)
            if err := retryQueue.Push(msg); err != nil {
                log.Printf("将消息加入重试队列失败: %v", err)
            }
            continue
        }

        ch, err := amqpConn.Channel()
        if err != nil {
            amqpConn.Close()
            log.Printf("创建通道错误: %v", err)
            if err := retryQueue.Push(msg); err != nil {
                log.Printf("将消息加入重试队列失败: %v", err)
            }
            continue
        }

        if err := publishMessage(ch, msg); err != nil {
            log.Printf("发布消息错误: %v", err)
            if err := retryQueue.Push(msg); err != nil {
                log.Printf("将消息加入重试队列失败: %v", err)
            }
        } else {
            log.Printf("消息已发送: %s", msg.Message)
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
            log.Printf("重试连接 RabbitMQ 错误: %v", err)
            retryQueue.Push(msg) 
            time.Sleep(time.Second * 5)
            continue
        }

        ch, err := amqpConn.Channel()
        if err != nil {
            amqpConn.Close()
            log.Printf("重试创建通道错误: %v", err)
            retryQueue.Push(msg) 
            time.Sleep(time.Second * 5)
            continue
        }

        if err := publishMessage(ch, msg); err != nil {
            log.Printf("重试发布消息错误: %v", err)
            retryQueue.Push(msg) 
        } else {
            log.Printf("重试消息发送成功: %s", msg.Message)
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
        log.Fatalf("加载配置错误: %v", err)
    }

    retryQueue, err := NewRetryQueue(config.Queue.MaxSize)
    if err != nil {
        log.Fatalf("创建重试队列错误: %v", err)
    }

    
    go retryWorker(retryQueue)

    addr := fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port)
    listener, err := net.Listen("tcp", addr)
    if err != nil {
        log.Fatalf("启动服务器错误: %v", err)
    }
    defer listener.Close()

    log.Printf("服务器启动在 %s", addr)

    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Printf("接受连接错误: %v", err)
            continue
        }
        go handleConnection(conn, retryQueue)
    }
} 