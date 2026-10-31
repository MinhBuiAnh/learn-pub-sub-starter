package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil { return err }

	if err := channel.Qos(10, 0, false); err != nil {
		return fmt.Errorf("cannot set prefetch message count: %v", err)
	}

	deliveryChan, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil { return fmt.Errorf("cannot consume message: %v", err) }

	go func() {
		for delivery := range deliveryChan {
			message, err := unmarshaller(delivery.Body)
			if err != nil {
				fmt.Println(err)
				continue
			}

			ackType := handler(message)
			switch ackType {
			case TypeAck:
				delivery.Ack(false)
				fmt.Println("Ack called, message consumed")
				fmt.Print("> ")
			case TypeNackRequeue:
				delivery.Nack(false, true)
				fmt.Println("Nack called, message requeued")
				fmt.Print("> ")
			case TypeNackDiscard:
				delivery.Nack(false, false)
				fmt.Println("Nack called, message discarded")
				fmt.Print("> ")
			}
		}
	}()

	return nil
}