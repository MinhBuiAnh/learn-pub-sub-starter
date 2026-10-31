package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	url := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("connect to RabbitMQ: %s", err.Error())
	}
	defer connection.Close()
	fmt.Println("successfully connected to RabbitMQ")

	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("create channel: %s", err.Error())
	}

	if err = pubsub.SubscribeGob(
		connection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.QueueTypeDurable,
		func(gameLog routing.GameLog) pubsub.AckType {
			defer fmt.Print("> ")
			if err := gamelogic.WriteLog(gameLog); err != nil {
				fmt.Println(err)
				return pubsub.TypeNackRequeue
			}
			return pubsub.TypeAck
		},
	); err != nil {
		log.Fatalf("subscribe log message: %s", err.Error())
	}

	gamelogic.PrintServerHelp()
	for ; ; {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("sending a pause message...")
			if err := pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			}); err != nil {
				log.Fatalf("publish message: %s", err.Error())
			}
			fmt.Println("pause message sent successfully")
		case "resume":
			fmt.Println("sending a resume message...")
			if err := pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			}); err != nil {
				log.Fatalf("publish message: %s", err.Error())
			}
			fmt.Println("resume message sent successfully")
		case "quit":
			fmt.Println("quitting the game...")
			defer fmt.Println("RabbitMQ connection closed")
			return
		default:
			fmt.Printf("unknown command: %s\n", words[0])
		}
	}
}
