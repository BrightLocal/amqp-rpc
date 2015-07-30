package rpc

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// Arguments: contentType, body. Return values: contentType, response
type RPCHandler func(string, []byte) (string, []byte)

type RPCServer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	messages   <-chan amqp.Delivery
	shutdown   chan struct{}
	handlers   map[string]RPCHandler
}

type rawRpcMessage struct {
	Cmd     string `json:"cmd"`
	Payload []byte `json:"payload"`
}

func NewServer(dsn, name string) (*RPCServer, error) {
	rpc := &RPCServer{
		handlers: make(map[string]RPCHandler),
		shutdown: make(chan struct{}),
	}
	var err error
	rpc.connection, err = amqp.Dial(dsn)
	if err != nil {
		return nil, err
	}
	rpc.channel, err = rpc.connection.Channel()
	if err != nil {
		return nil, err
	}
	q, err := rpc.channel.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}
	err = rpc.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return nil, err
	}
	rpc.messages, err = rpc.channel.Consume(
		q.Name, // queue
		"",     // consume
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}
	go rpc.run()
	return rpc, nil
}

func (r *RPCServer) AddHandler(name string, fn RPCHandler) *RPCServer {
	r.handlers[name] = fn
	return r
}

func (r *RPCServer) RemoveHandler(name string) bool {
	if _, ok := r.handlers[name]; ok {
		delete(r.handlers, name)
		return true
	}
	return false
}

func (r *RPCServer) Shutdown() error {
	r.shutdown <- struct{}{}
	if err := r.channel.Close(); err != nil {
		return err
	}
	if err := r.connection.Close(); err != nil {
		return err
	}
	r = nil
	return nil
}

func (r *RPCServer) run() {
	for {
		select {
		case <-r.shutdown:
			return
		case d := <-r.messages:
			var msg rawRpcMessage
			var response []byte
			contentType := "text/json"
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				response = []byte(fmt.Sprintf(`{"result": false, "error": %q}`, err))
			} else {
				if fn, ok := r.handlers[msg.Cmd]; ok {
					contentType, response = fn(d.ContentType, msg.Payload)
				}
			}
			if response != nil {
				if err := r.channel.Publish(
					"",        // exchange
					d.ReplyTo, // routing key
					false,     // mandatory
					false,     // immediate
					amqp.Publishing{
						ContentType:   contentType,
						CorrelationId: d.CorrelationId,
						Body:          []byte(response),
					}); err != nil {
					log.Printf("Error sending response: %s", err)
				}
			}
			d.Ack(false)
		}
	}
}
