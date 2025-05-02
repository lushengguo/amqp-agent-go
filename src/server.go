package main

import (
	"encoding/json"
	"github.com/bcicen/jstream"
	"net"
)

type Message struct {
	URL          string `json:"url"`
	Exchange     string `json:"exchange"`
	ExchangeType string `json:"exchange_type"`
	RoutingKey   string `json:"routing_key"`
	Message      string `json:"m"`
	Timestamp    uint32 `json:"timestamp"`
	Queue        string `json:"queue"`
}

func (m *Message) Locator() string {
	return m.URL + " " + m.Exchange + " " + m.RoutingKey + " " + m.Queue
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := jstream.NewDecoder(conn, 0)

	for streamObj := range decoder.Stream() {
		jsonData, err := json.Marshal(streamObj.Value)
		if err != nil {
			GetLogger().Errorf("error re-marshaling JSON: %v", err)
			continue
		}

		var m *Message
		if err := json.Unmarshal(jsonData, &m); err != nil {
			GetLogger().Errorf("error parsing JSON: %v, data: %s", err, string(jsonData))
			continue
		}

		GetLogger().Debugf("Received m: %+v", m)
		go func(m *Message) {
			Produce(m)
		}(m)
	}
}
