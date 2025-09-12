package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
