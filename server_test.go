package rpc

import (
	"log"
	"testing"
)

func TestRPC(t *testing.T) {
	t.SkipNow()
	rpc := NewServer("amqp://guest:guest@localhost:5672/", "rpc_queue")
	rpc.AddHandler("test", myHandler)
	finish := make(chan bool)
	<-finish
}

func myHandler(contentType string, input []byte) (string, []byte) {
	log.Print("Handler here")
	return contentType, input
}
