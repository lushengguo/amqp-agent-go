package main

import (
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var connectionManager = NewAmqpConnectionManager()

type AMQPConnection struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	mu   sync.Mutex
}

type AmqpConnectionManager struct {
	connections map[string]*AMQPConnection
	mu          sync.Mutex
}

func NewAmqpConnectionManager() *AmqpConnectionManager {
	return &AmqpConnectionManager{
		connections: make(map[string]*AMQPConnection),
	}
}

func (manager *AmqpConnectionManager) CreateConnection(m *Message) error {
	{
		manager.mu.Lock()
		defer manager.mu.Unlock()

		conn := manager.connections[m.Locator()]
		if conn != nil {
			return nil
		}
	}

	// have convinced that connection is nil and construct it now
	config := amqp.Config{
		Vhost:      "/",
		Properties: amqp.NewConnectionProperties(),
	}
	config.Properties.SetClientConnectionName("producer-with-confirms")

	GetLogger().Infof("producer: dialing %s", m.Locator())
	conn, err := amqp.DialConfig(m.Locator(), config)
	if err != nil {
		GetLogger().Fatalf("producer: error in dial: %s", err)
	}
	defer conn.Close()

	GetLogger().Println("producer: got Connection, getting Channel")
	channel, err := conn.Channel()
	if err != nil {
		GetLogger().Fatalf("error getting a channel: %s", err)
	}
	defer channel.Close()

	GetLogger().Infof("producer: declaring exchange")
	if err := channel.ExchangeDeclare(
		m.Exchange,     // name
		m.ExchangeType, // type
		true,           // durable
		false,          // auto-delete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		GetLogger().Fatalf("producer: Exchange Declare: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	GetLogger().Infof("producer: enabling publisher confirms.")
	if err := channel.Confirm(false); err != nil {
		GetLogger().Fatalf("producer: channel could not be put into confirm mode: %s", err)
	}

	// save to map
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.connections[m.Locator()] = &AMQPConnection{
		conn: conn,
		ch:   channel,
	}

	return nil
}

func (manager *AmqpConnectionManager) GetConnection(m *Message) *amqp.Channel {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	// during connection construct will place a nil to the map
	// which could be treat as failure for outer caller
	// they should retry later
	if conn, exists := manager.connections[m.Locator()]; exists {
		return conn.ch
	} else {
		manager.connections[m.Locator()] = nil
		go func() {
			err := manager.CreateConnection(m)

			// if async create connection failed, remove it and it will retry later
			if err != nil {
				manager.mu.Lock()
				defer manager.mu.Unlock()
				delete(manager.connections, m.Locator())
			}
		}()
	}

	return nil
}

func (manager *AmqpConnectionManager) CloseConnection(url string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if amqpConn, exists := manager.connections[url]; exists {
		amqpConn.mu.Lock()
		if amqpConn.ch != nil {
			amqpConn.ch.Close()
		}
		if amqpConn.conn != nil {
			amqpConn.conn.Close()
		}
		amqpConn.mu.Unlock()
		delete(manager.connections, url)
	}
}

func Produce(m *Message) {
	GetStatistic().IncrementProduced()

	if m.Locator() == "" {
		GetLogger().Errorf("URL is empty, cannot produce m: %s", m.Message)
		return
	}

	ch := connectionManager.GetConnection(m)
	if ch == nil {
		GetRetryQueue().Push(m)
		return
	}

	if err := produceMessage(ch, m); err != nil {
		GetLogger().Errorf("error producing m: %v", err)
		GetRetryQueue().Push(m)
		return
	}

	GetStatistic().IncrementConfirmed()
}

func produceMessage(ch *amqp.Channel, m *Message) error {
	GetLogger().Infof("producer: publishing %dB body (%q)", len(m.Message), m.Message)
	confirmCh, err := ch.PublishWithDeferredConfirm(
		m.Exchange,
		m.RoutingKey,
		true,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			DeliveryMode:    amqp.Persistent,
			Priority:        0,
			AppId:           "sequential-producer",
			Body:            []byte(m.Message),
		},
	)
	if err != nil {
		GetLogger().Fatalf("producer: error in publish: %s", err)
		return err
	}

	time.Sleep(time.Second * 10)
	if confirmCh.Acked() {
		return nil
	} else {
		GetLogger().Warnf("produce %s m not acked: timeout", m.Message)
		return fmt.Errorf("produce timeout")
	}
}

func PeriodicallyReproduceFailedMessage() {
	for {
		if GetRetryQueue().IsEmpty() {
			time.Sleep(time.Second)
			continue
		}

		m := GetRetryQueue().Pop()
		if m == nil {
			continue
		}

		Produce(m)

		time.Sleep(time.Second)
	}
}
