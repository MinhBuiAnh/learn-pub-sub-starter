package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var network bytes.Buffer
	encoder := gob.NewEncoder(&network)
	if err := encoder.Encode(val); err != nil {
		return fmt.Errorf("cannot encode data: %v", err)
	}

	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body: network.Bytes(),
	}); err != nil {
		return fmt.Errorf("cannot publish message: %v", err)
	}

	return nil
}