package rpc

import (
	"bytes"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	const qName = "rpc_test"
	const dsn = "amqp://guest:guest@localhost:5672/"
	rpc, err := NewServer(dsn, qName)
	if err != nil {
		t.Errorf("Error creating server instance: %s", err)
	}
	rpc.AddHandler("test", myClientHandler)
	c, _ := NewClient(dsn, qName, "text/plain")
	c.Timeout = time.Second
	resp, err := c.Call([]byte(`{"cmd": "test", "payload": [65,66,67]}`))
	if err != nil {
		t.Errorf("Error getting response: %s", err)
	}
	if bytes.Compare(resp, []byte(`ABC`)) != 0 {
		t.Errorf("Expected ABC, got %s", resp)
	}
}

func myClientHandler(contentType string, input []byte) (string, []byte) {
	return contentType, input
}
