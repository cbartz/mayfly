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
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	PublishWithDeferredConfirm(exchange string, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error)
	Close() error
}

type AmqpConnection interface {
	Channel() (*AmqpChannel, error)
	Close() error
}

type ConfirmHandler func(confirmationChan chan bool, deferredConfirmation *amqp.DeferredConfirmation)
type ConnectFunc func(uri string) (AmqpChannel, chan *amqp.Error, error)

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

		err := Producer(q, q.ProducerChan, make(chan bool), connect, confirmHandler)
		if err != nil {
			log.Panicf("Producer error: %s", err)
		}
	}()
}

func connect(uri string) (AmqpChannel, chan *amqp.Error, error) {
	conn, err := amqp.Dial(uri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	ch.Confirm(false)
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	errChan := make(chan *amqp.Error, 1)
	ch.NotifyClose(errChan)
	return ch, errChan, nil
}

func Producer(q *AmqpQueue, producerChan chan ProduceMsg, shutdownChan chan bool, connectFunc ConnectFunc, confirmHandlerFunc ConfirmHandler) error {

	amqpChannel, connErrorChan, err := connectFunc(q.URI)
	if err != nil {
		return errors.New("Failed to connect to RabbitMQ: " + err.Error())
	}

	_, err = amqpChannel.QueueDeclare(
		q.Name, // name
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		return errors.New("Failed to declare a queue: " + err.Error())
	}

	for {

		var produceMsg ProduceMsg
		select {

		case produceMsg = <-producerChan:
		case _ = <-shutdownChan:
			return nil
		case err := <-connErrorChan:
			log.Println("Connection error:", err)
			var connectErr error
			amqpChannel, connErrorChan, connectErr = connectFunc(q.URI)
			if connectErr != nil {
				return errors.New("Failed to reconnect to RabbitMQ: " + connectErr.Error())
			}
		}

		deferred_confirm, err := amqpChannel.PublishWithDeferredConfirm(
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

		go confirmHandlerFunc(produceMsg.confirmationChan, deferred_confirm)
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
	}
	return nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
