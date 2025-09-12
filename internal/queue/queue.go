package queue

import (
	"context"
	"errors"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue interface {
	Push([]byte) error
}

type AmqpChannel interface {
	PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (deferred amqp.DeferredConfirmation, err error)
	Close() error
}

type AmqpConnection interface {
	Channel() (*AmqpChannel, error)
	Close() error
}

type ProduceMsg struct {
	msg              []byte
	confirmationChan chan bool
}
type AmqpQueue struct {
	// Add fields for AMQP connection, channel, etc.
	URI          string
	Name         string
	ProducerChan chan ProduceMsg
}

func NewAmqpQueue(uri, name string) *AmqpQueue {
	return &AmqpQueue{
		URI:          uri,
		Name:         name,
		ProducerChan: make(chan ProduceMsg),
	}
}

func (q *AmqpQueue) StartProducer() {
	go func() {
		err := Producer(q, q.ProducerChan)
		if err != nil {
			log.Panicf("Producer error: %s", err)
		}
	}()
}

func Producer(q *AmqpQueue, producerChan chan ProduceMsg) error {

	conn, err := amqp.Dial(q.URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	ch.Confirm(false)
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	_, err = ch.QueueDeclare(
		q.Name, // name
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for {

		produceMsg := <-producerChan

		deferred_confirm, err := ch.PublishWithDeferredConfirm(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        produceMsg.msg,
			},
		)
		if err != nil {
			// if publish fails, we should notify the caller that the message was not confirmed
			produceMsg.confirmationChan <- false
			log.Println("Failed to publish message:", err)
			continue // skip to next message
		}
		log.Println("Waiting for confirmation...", deferred_confirm)

		failOnError(err, "Failed to publish") // TODO add error distinguishment and reconnection logic

		go confirmHandler(produceMsg.confirmationChan, deferred_confirm)
	}
	return nil
}

func confirmHandler(confirmationChan chan bool, deferredConfirmation *amqp.DeferredConfirmation) {
	timeout, _ := context.WithTimeout(context.Background(), 5*time.Second)
	log.Println("Waiting for confirmation...", deferredConfirmation)
	confirmation, _ := deferredConfirmation.WaitContext(timeout)
	log.Println("Confirmation received:", confirmation)
	confirmationChan <- confirmation
}

func (q *AmqpQueue) Push(msg []byte) error {
	confirmationChan := make(chan bool)
	channel_msg := ProduceMsg{
		msg:              msg,
		confirmationChan: confirmationChan,
	}

	q.ProducerChan <- channel_msg

	confirmation := <-confirmationChan

	if confirmation != true {
		return errors.New("message not confirmed")
		// raise error
	}
	return nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
