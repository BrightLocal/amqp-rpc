package rpc

import (
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type RPCClient struct {
	name        string
	dsn         string
	contentType string
	connection  *amqp.Connection
	channel     *amqp.Channel
	queue       amqp.Queue
	Timeout     time.Duration
	log         *log.Logger
}

var ErrTimeout = errors.New("Message receive timeout")

func NewClient(dsn, name, contentType string) (*RPCClient, error) {
	rpc := &RPCClient{
		name:    name,
		dsn:     dsn,
		Timeout: 15 * time.Second,
		log:     log.New(os.Stdout, "[RPC Client] ", log.LstdFlags),
	}
	return rpc, nil
}

func (r *RPCClient) connect() {
	var err error
	for {
		if r.connection == nil {
			r.connection, err = amqp.DialConfig(r.dsn, amqp.Config{Properties: amqp.Table{"product": "RPC/Client." + r.name}})
			if err != nil {
				r.log.Printf("Error connecting: %s", err)
				time.Sleep(time.Second)
				continue
			}
		}
		r.channel, err = r.connection.Channel()
		if err != nil {
			r.log.Printf("Error getting channel: %s", err)
			time.Sleep(time.Second)
			continue
		}
		r.queue, err = r.channel.QueueDeclare(
			"",    // name
			false, // durable
			true,  // delete when unused
			true,  // exclusive
			false, // noWait
			nil,   // arguments
		)
		if err != nil {
			r.log.Printf("Error declaring a queue: %s", err)
			time.Sleep(time.Second)
			continue
		}
		break
	}
}

func (r *RPCClient) getCorrelationId() string {
	bytes := make([]byte, 32)
	for i := 0; i < 32; i++ {
		bytes[i] = byte(65 + rand.Intn(25))
	}
	return string(bytes)
}

func (r *RPCClient) Call(command string, input []byte) ([]byte, error) {
	body, err := json.Marshal(rawRpcMessage{
		Cmd:     command,
		Payload: input,
	})
	if err != nil {
		return nil, err
	}
	for {
		r.connect()
		correlationId := r.getCorrelationId()
		err = r.channel.Publish(
			"",     // exchange
			r.name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType:   r.contentType,
				CorrelationId: correlationId,
				ReplyTo:       r.queue.Name,
				Body:          body,
			},
		)
		if err != nil {
			r.log.Printf("Error publishing: %s", err)
			r.connect()
			continue
		}
		deliveries, err := r.channel.Consume(
			r.queue.Name, // queue
			"",           // consume
			true,         // auto-ack
			false,        // exclusive
			false,        // no-local
			false,        // no-wait
			nil,          // args
		)
		if err != nil {
			r.log.Printf("Error consuming: %s", err)
			r.connect()
			continue
		}
		timeout := make(chan struct{})
		go func() {
			time.Sleep(r.Timeout)
			timeout <- struct{}{}
		}()
		for {
			select {
			case d := <-deliveries:
				if correlationId == d.CorrelationId {
					d.Ack(false)
					r.channel.Close()
					for range deliveries {
					}
					return d.Body, nil
				}
				d.Nack(false, true)
			case <-timeout:
				return nil, ErrTimeout
			}
		}
	}
	return nil, ErrTimeout
}

func (r *RPCClient) Shutdown() error {
	return r.connection.Close()
}
