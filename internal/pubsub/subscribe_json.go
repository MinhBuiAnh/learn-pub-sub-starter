package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
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
			var message T
			if err := json.Unmarshal(b, &message); err != nil {
				return message, fmt.Errorf("cannot unmarshal data: %v", err)
			}

			return message, nil
		},
	)
}