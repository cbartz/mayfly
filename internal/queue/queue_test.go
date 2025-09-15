package queue

import (
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

const QueueWithDeclareError = "queue-with-declare-error"

type MockAmqpChannel struct {
	queueName         string
	publishedMessages [][]byte
	closed            bool
}

type MockAmqConnection struct {
	closed bool
}

func (ch *MockAmqpChannel) PublishWithDeferredConfirm(exchange string, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	ch.publishedMessages = append(ch.publishedMessages, msg.Body)
	return nil, nil
}

func (ch *MockAmqpChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if name == QueueWithDeclareError {
		return amqp.Queue{}, errors.New("failed to declare queue")
	}
	ch.queueName = name
	return amqp.Queue{}, nil
}

func (ch *MockAmqpChannel) Close() error {
	ch.closed = true
	return nil
}

func (c *MockAmqConnection) Close() error {
	c.closed = true
	return nil
}

func mockConfirmHandlerSuccess(confirmationChan chan bool, deferredConfirmation *amqp.DeferredConfirmation) {
	go func() {
		// Simulate a successful confirmation
		confirmationChan <- true
	}()
}

func mockConfirmHandlerFailure(confirmationChan chan bool, deferredConfirmation *amqp.DeferredConfirmation) {
	go func() {
		// Simulate a failed confirmation
		confirmationChan <- false
	}()
}

func TestPushWithSuccess(t *testing.T) {
	/*
		arrange: create a fake AmqpQueue with a producer that always confirms
		act: push a message to the queue
		assert: no error is returned
	*/
	producerChan := make(chan ProduceMsg, 1)
	fakeQueue := AmqpQueue{
		URI:          "amqp://guest:guest@localhost:5672/",
		Name:         "test-queue",
		ProducerChan: producerChan,
	}

	go func() {
		produceMsg := <-producerChan
		produceMsg.confirmationChan <- true
	}()

	err := fakeQueue.Push([]byte("test message"))

	assert.NoError(t, err)
}

func TestPushWithFailure(t *testing.T) {
	/*
		arrange: create a fake AmqpQueue with a producer that always fails to confirm
		act: push a message to the queue
		assert: an error is returned
	*/
	producerChan := make(chan ProduceMsg, 1)
	fakeQueue := AmqpQueue{
		URI:          "amqp://guest:guest@localhost:5672/",
		Name:         "test-queue",
		ProducerChan: producerChan,
	}

	go func() {
		produceMsg := <-producerChan
		produceMsg.confirmationChan <- false
	}()

	err := fakeQueue.Push([]byte("test message"))

	assert.Error(t, err)
	assert.ErrorContains(t, err, "message not confirmed")
}

func TestProducerDeclaresQueue(t *testing.T) {
	producerChan := make(chan ProduceMsg, 1)
	shutdownChan := make(chan bool, 1)
	fakeQueue := AmqpQueue{
		URI:          "amqp://guest:guest@localhost:5672/",
		Name:         "test-queue",
		ProducerChan: producerChan,
	}

	fakeAmqpChan := MockAmqpChannel{}
	connectFunc := (func(uri string) (AmqpChannel, chan *amqp.Error, error) {
		return &fakeAmqpChan, make(chan *amqp.Error), nil
	})

	shutdownChan <- true
	err := Producer(&fakeQueue, producerChan, shutdownChan, connectFunc, mockConfirmHandlerSuccess)
	assert.NoError(t, err)
	assert.Equal(t, "test-queue", fakeAmqpChan.queueName, "queue name should match")
}

func TestProducerPublishesMessage(t *testing.T) {
	producerChan := make(chan ProduceMsg, 1)
	shutdownChan := make(chan bool, 1)
	fakeQueue := AmqpQueue{
		URI:          "amqp://guest:guest@localhost:5672/",
		Name:         "test-queue",
		ProducerChan: producerChan,
	}

	fakeAmqpChan := MockAmqpChannel{}
	connectFunc := (func(uri string) (AmqpChannel, chan *amqp.Error, error) {
		return &fakeAmqpChan, make(chan *amqp.Error), nil
	})
	confirmationChan := make(chan bool, 1)
	produceMsg := ProduceMsg{
		msg:              []byte("test message"),
		confirmationChan: confirmationChan,
	}
	producerChan <- produceMsg

	go Producer(&fakeQueue, producerChan, shutdownChan, connectFunc, mockConfirmHandlerSuccess)

	assert.True(t, <-confirmationChan, "message should be confirmed")
	assert.Contains(t, fakeAmqpChan.publishedMessages, []byte("test message"), "published messages should contain the test message")

	shutdownChan <- true
}

func TestProducerCouldNotPublishMessage(t *testing.T) {
	producerChan := make(chan ProduceMsg, 1)
	shutdownChan := make(chan bool, 1)
	fakeQueue := AmqpQueue{
		URI:          "amqp://guest:guest@localhost:5672/",
		Name:         "test-queue",
		ProducerChan: producerChan,
	}

	fakeAmqpChan := MockAmqpChannel{}
	connectFunc := (func(uri string) (AmqpChannel, chan *amqp.Error, error) {
		return &fakeAmqpChan, make(chan *amqp.Error), nil
	})
	confirmationChan := make(chan bool, 1)
	produceMsg := ProduceMsg{
		msg:              []byte("test message"),
		confirmationChan: confirmationChan,
	}
	producerChan <- produceMsg

	go Producer(&fakeQueue, producerChan, shutdownChan, connectFunc, mockConfirmHandlerFailure)

	assert.False(t, <-confirmationChan, "message should not be confirmed")

	shutdownChan <- true
}

func TestProducerChannelShutDownTriesReconnect(t *testing.T) {
	producerChan := make(chan ProduceMsg, 1)
	shutdownChan := make(chan bool, 1)
	fakeQueue := AmqpQueue{
		URI:          "amqp://guest:guest@localhost:5672/",
		Name:         "test-queue",
		ProducerChan: producerChan,
	}
	fakeAmqpChan := MockAmqpChannel{}
	errorChan := make(chan *amqp.Error, 1)
	connectCalls := 0
	connectFunc := (func(uri string) (AmqpChannel, chan *amqp.Error, error) {
		connectCalls++
		return &fakeAmqpChan, errorChan, nil
	})
	errorChan <- amqp.ErrClosed

	go Producer(&fakeQueue, producerChan, shutdownChan, connectFunc, mockConfirmHandlerSuccess)

	assert.Eventually(t, func() bool {
		return connectCalls >= 2
	}, time.Second, 1, "connect should be called multiple times")
	shutdownChan <- true

}

func TestProducerConnectError(t *testing.T) {
	producerChan := make(chan ProduceMsg, 1)
	shutdownChan := make(chan bool, 1)
	fakeQueue := AmqpQueue{
		URI:          "amqp://guest:guest@localhost:5672/",
		Name:         "test-queue",
		ProducerChan: producerChan,
	}
	fakeAmqpChan := MockAmqpChannel{}
	connectFunc := (func(uri string) (AmqpChannel, chan *amqp.Error, error) {
		return &fakeAmqpChan, nil, errors.New("error")
	})

	err := Producer(&fakeQueue, producerChan, shutdownChan, connectFunc, mockConfirmHandlerSuccess)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Failed to connect to RabbitMQ")
}

func TestProducerQueueDeclareError(t *testing.T) {
	producerChan := make(chan ProduceMsg, 1)
	shutdownChan := make(chan bool, 1)
	fakeQueue := AmqpQueue{
		URI:          "amqp://guest:guest@localhost:5672/",
		Name:         QueueWithDeclareError,
		ProducerChan: producerChan,
	}
	fakeAmqpChan := MockAmqpChannel{}
	connectFunc := (func(uri string) (AmqpChannel, chan *amqp.Error, error) {
		return &fakeAmqpChan, make(chan *amqp.Error), nil
	})

	err := Producer(&fakeQueue, producerChan, shutdownChan, connectFunc, mockConfirmHandlerSuccess)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Failed to declare a queue")
}

func TestProducerClosesResources(t *testing.T) {
	producerChan := make(chan ProduceMsg, 1)
	shutdownChan := make(chan bool, 1)
	fakeQueue := AmqpQueue{
		URI:          "amqp://guest:guest@localhost:5672/",
		Name:         "test-queue",
		ProducerChan: producerChan,
	}
	fakeAmqpChan := MockAmqpChannel{}
	fakeAmqpConn := MockAmqConnection{
		closed: false,
	}
	connectFunc := (func(uri string) (AmqpConnection, AmqpChannel, chan *amqp.Error, error) {
		return &fakeAmqpConn, &fakeAmqpChan, make(chan *amqp.Error), nil
	})

	go Producer(&fakeQueue, producerChan, shutdownChan, connectFunc, mockConfirmHandlerSuccess)

	shutdownChan <- true

	assert.Eventually(t,
		func() bool {
			return fakeAmqpConn.closed
		}, time.Second, 1, "connection should be closed")

	assert.Eventually(t,
		func() bool {
			return fakeAmqpChan.closed
		}, time.Second, 1, "channel should should be closed")

}
