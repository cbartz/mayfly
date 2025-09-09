package queue

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue interface {
	Close() error
	Connect() error
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

type AmqpQueue struct {
	// Add fields for AMQP connection, channel, etc.
	URI  string
	Name string
	Conn *AmqpConnection
}

func (q *AmqpQueue) Close() error {
	return (*q.Conn).Close()
}

//func (q *AmqpQueue) Connect() error {
//	conn, err := amqp.Dial(q.URI)
//	if err != nil {
//		return err
//	}
//	q.Conn = conn
//	return nil
//}
//
//func NewAmqpQueue(uri, name string) (*AmqpQueue, error) {
//	conn, err := amqp.Dial(uri)
//	if err != nil {
//		return nil, err
//	}
//
//	ch, err := conn.Channel()
//	if err != nil {
//		conn.Close()
//		return nil, err
//	}
//	defer ch.Close()
//
//	_, err = ch.QueueDeclare(
//		name,  // name
//		true,  // durable
//		false, // delete when unused
//		false, // exclusive
//		false, // no-wait
//		nil,   // arguments
//	)
//	if err != nil {
//		ch.Close()
//		conn.Close()
//		return nil, err
//	}
//
//	return &AmqpQueue{
//		URI:  uri,
//		Name: name,
//		Conn: conn,
//	}, nil
//}

//func (q *AmqpQueue) Push(msg []byte) error {
//	ch, err := q.Conn.Channel()
//	if err != nil {
//		ch.Close()
//		return err
//	}
//	defer ch.Close()
//	deferred_confirm, _ := ch.PublishWithDeferredConfirm(
//		"",     // exchange
//		q.Name, // routing key
//		false,  // mandatory
//		false,  // immediate
//		amqp.Publishing{
//			ContentType: "application/json",
//			Body:        msg,
//		},
//	)
//	// Wait for confirmation using timeout
//	timeout, _ := context.WithTimeout(context.Background(), 5*time.Second)
//	deferred_confirm.WaitContext(timeout)
//
//	return nil
//}
