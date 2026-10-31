package pubsub

import (
	"bytes"
	"encoding/gob"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeGob[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType, // an enum to represent "durable" or "transient"
    handler func(T) AckType,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(b []byte) (T, error) {
			var network bytes.Buffer
			var message T

			_, err := network.Write(b)
			if err != nil {
				return message, fmt.Errorf("cannot write to buffer: %v", err)
			}

			decoder := gob.NewDecoder(&network)
			if err := decoder.Decode(&message); err != nil {
				return message, fmt.Errorf("cannot decode data: %v", err)
			}

			return message, nil
		},
	)
}