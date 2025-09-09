package queue

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type FakeAmqpChannel struct {
	mock.Mock
}

func (f FakeAmqpChannel) PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (deferred amqp.DeferredConfirmation, err error) {
	f.Mock.Called(exchange, key, mandatory, immediate, msg)
	return amqp.DeferredConfirmation{}, nil
}

func (f FakeAmqpChannel) Close() error {
	f.Mock.Called()
	return nil
}

type FakeAmqpConnection struct {
	mock.Mock
}

func (f FakeAmqpConnection) Channel() (*AmqpChannel, error) {
	var amqpChannel AmqpChannel
	amqpChannel = FakeAmqpChannel{}
	return &amqpChannel, nil
}

func (f FakeAmqpConnection) Close() error {
	f.Mock.Called()
	return nil
}

func TestClose(t *testing.T) {
	fakeAmqpConnection := FakeAmqpConnection {}
	amqpConnection.On("Close").Return(nil)
	queue := &AmqpQueue{
		URI:  "amqp://guest:guest@localhost:5672/",
		Name: "test-queue",
		Conn: &((AmqpConnection) amqpConnection),
	}
	err := queue.Close()
	assert.NoError(t, err)
}

type MockedQueue struct {
	mock.Mock
}

//func TestNewAmqpQueue(t *testing.T) {
//	// This is a placeholder test to ensure the package compiles and can be tested.
//	// Actual tests would require a running AMQP server and are beyond the scope of this example.
//	t.Log("NewAmqpQueue function exists and can be called")
//
//	_, err := NewAmqpQueue("amqp://guest:guest@localhost:5672/", "test-queue")
//	if err != nil {
//		t.Logf("Expected error when connecting to non-existent AMQP server: %v", err)
//	} else {
//		t.Error("Expected an error when connecting to non-existent AMQP server, but got none")
//	}
//}
