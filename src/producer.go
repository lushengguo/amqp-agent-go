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
	// have convinced that connection is nil and construct it now
	config := amqp.Config{
		Vhost:      "/",
		Properties: amqp.NewConnectionProperties(),
	}
	config.Properties.SetClientConnectionName("producer-with-confirms")

	GetLogger().Infof("producer: dialing %s", m.Locator())
	conn, err := amqp.DialConfig(m.URL, config)
	if err != nil {
		GetLogger().Errorf("producer: error in dial: %s", err)
		return err
	}

	GetLogger().Println("producer: got Connection, getting Channel")
	channel, err := conn.Channel()
	if err != nil {
		GetLogger().Errorf("error getting a channel: %s", err)
		return err
	}

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
		GetLogger().Errorf("producer: Exchange Declare: %s", err)
		return err
	}

	if len(m.Queue) != 0 {
		GetLogger().Infof("producer: declaring queue '%s'", m.Queue)
		queue, err := channel.QueueDeclare(
			m.Queue, // name of the queue
			true,    // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // noWait
			nil,     // arguments
		)
		if err == nil {
			GetLogger().Infof("producer: declared queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
				queue.Name, queue.Messages, queue.Consumers, m.RoutingKey)
		} else {
			GetLogger().Errorf("producer: Queue Declare: %s", err)
			return err
		}

		GetLogger().Infof("producer: declaring binding")
		if err := channel.QueueBind(queue.Name, m.RoutingKey, m.Exchange, false, nil); err != nil {
			GetLogger().Errorf("producer: Queue Bind: %s", err)
			return err
		}
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	GetLogger().Infof("producer: enabling publisher confirms.")
	if err := channel.Confirm(false); err != nil {
		GetLogger().Errorf("producer: channel could not be put into confirm mode: %s", err)
		return err
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
		defer amqpConn.mu.Unlock()
		if amqpConn.ch != nil {
			amqpConn.ch.Close()
		}
		if amqpConn.conn != nil {
			amqpConn.conn.Close()
		}
		delete(manager.connections, url)
	}
}

func Produce(m *Message) {
	GetStatistic().IncrementProduced(m.Locator())

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

	GetStatistic().IncrementConfirmed(m.Locator())
}

func produceMessage(ch *amqp.Channel, m *Message) error {
	GetLogger().Debugf("producing message to %s, body:(%q) ", m.Locator(), m.Message)

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
		GetLogger().Errorf("producer: error in publish: %s", err)
		return err
	}

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	timeout := time.After(10 * time.Second)

	for {
		select {
		case <-ticker.C:
			if confirmCh.Acked() {
				GetLogger().Debugf("confirmed message to %s, body:(%q) ", m.Locator(), m.Message)
				return nil
			}
		case <-timeout:
			GetLogger().Warnf("produce %s message not acked: timeout", m.Message)
			return fmt.Errorf("produce timeout")
		}
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
