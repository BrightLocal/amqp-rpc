package rpc

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Arguments: contentType, body. Return values: contentType, response
type RPCHandler func(string, []byte) (string, []byte)

type RPCServer struct {
	dsn        string
	queueName  string
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

func NewServer(dsn, name string) *RPCServer {
	rpc := &RPCServer{
		dsn:       dsn,
		queueName: name,
		handlers:  make(map[string]RPCHandler),
		shutdown:  make(chan struct{}),
	}
	rpc.connect()
	go rpc.run()
	return rpc
}

func (rpc *RPCServer) connect() {
	var err error
	for {
		rpc.connection, err = amqp.Dial(rpc.dsn)
		if err != nil {
			log.Printf("Error connecting: %s", err)
			time.Sleep(time.Second)
			continue
		}
		rpc.channel, err = rpc.connection.Channel()
		if err != nil {
			log.Printf("Error getting channel: %s", err)
			time.Sleep(time.Second)
			continue
		}
		q, err := rpc.channel.QueueDeclare(
			rpc.queueName, // name
			false,         // durable
			false,         // delete when unused
			false,         // exclusive
			false,         // noWait
			nil,           // arguments
		)
		if err != nil {
			log.Printf("Error declaring queue: %s", err)
			time.Sleep(time.Second)
			continue
		}
		err = rpc.channel.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		if err != nil {
			log.Printf("Error setting QOS: %s", err)
			time.Sleep(time.Second)
			continue
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
			log.Printf("Error consuming: %s", err)
			time.Sleep(time.Second)
			continue
		}
		break
	}
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
				response = []byte(fmt.Sprintf(`{"success": false, "error": %q}`, err))
				// Broken message, discard
				d.Ack(false)
			} else {
				if fn, ok := r.handlers[msg.Cmd]; ok {
					d.Ack(false)
					contentType, response = fn(d.ContentType, msg.Payload)
				} else {
					// Handler not registered, requeue the message
					d.Nack(false, true)
					continue
				}
			}
			if response != nil {
				for {
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
						time.Sleep(time.Second)
						r.connect()
						continue
					}
					break
				}
			} else {
				log.Print("Not sending empty response")
			}
		}
	}
}
