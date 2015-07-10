package rpc

import (
	"errors"
	"github.com/streadway/amqp"
	"math/rand"
	"time"
)

type rpcClient struct {
	name        string
	contentType string
	connection  *amqp.Connection
	channel     *amqp.Channel
	queue       amqp.Queue
	Timeout     time.Duration
}

var ErrTimeout = errors.New("Message receive timeout")

func NewClient(dsn, name, contentType string) (*rpcClient, error) {
	rpc := &rpcClient{
		name:    name,
		Timeout: 15 * time.Second,
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
	rpc.queue, err = rpc.channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}
	return rpc, nil
}

func (r *rpcClient) getCorrelationId() string {
	bytes := make([]byte, 32)
	for i := 0; i < 32; i++ {
		bytes[i] = byte(65 + rand.Intn(25))
	}
	return string(bytes)
}

func (r *rpcClient) Call(input []byte) ([]byte, error) {
	correlationId := r.getCorrelationId()
	err := r.channel.Publish(
		"",     // exchange
		r.name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType:   r.contentType,
			CorrelationId: correlationId,
			ReplyTo:       r.queue.Name,
			Body:          input,
		},
	)
	if err != nil {
		return nil, err
	}
	msgs, err := r.channel.Consume(
		r.queue.Name, // queue
		"",           // consume
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return nil, err
	}
	timeout := make(chan struct{})
	go func() {
		time.Sleep(r.Timeout)
		timeout <- struct{}{}
	}()
	select {
	case d := <-msgs:
		if correlationId == d.CorrelationId {
			return d.Body, nil
		}
	case <-timeout:
		break
	}
	return nil, ErrTimeout
}
