package mqtt

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	ErrMqttPublishFailure = errors.New("mqtt publish failed")
)

type Config struct {
	BrokerURL string
	Port      uint
}

type Client struct {
	config *Config
	c      mqtt.Client
}

// New creates a client that uses the given RPC client.
func NewClient(config *Config) *Client {
	if config == nil {
		config = &Config{BrokerURL: "159.89.199.40",
			Port: 1883}
	}

	connectAddress := fmt.Sprintf("tcp://%s:%d", config.BrokerURL, config.Port)
	client_id := fmt.Sprintf("go-client-%d", rand.Int())

	opts := mqtt.NewClientOptions()

	opts.AddBroker(connectAddress)
	opts.SetClientID(client_id)
	opts.SetKeepAlive(60)

	client := mqtt.NewClient(opts)

	token := client.Connect()

	if token.WaitTimeout(3*time.Second) && token.Error() != nil {
		panic(token.Error())
	}

	return &Client{config, client}
}

func (c *Client) Publish(topic string, payload string) error {
	qos := 0

	if token := c.c.Publish(topic, byte(qos), false, payload); token.Wait() && token.Error() != nil {
		return ErrMqttPublishFailure
	}

	return nil
}
