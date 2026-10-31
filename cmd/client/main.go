package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("input username: %s", err.Error())
	}

	gameState := gamelogic.NewGameState(username)

	if err := pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, gameState.GetUsername()),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.QueueTypeTransient,
		handleMove(gameState, channel),
	); err != nil {
		log.Fatalf("subscribe move message: %v", err)
	}

	if err := pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.QueueTypeDurable,
		handleWar(gameState, channel),
	); err != nil {
		log.Fatalf("subscribe war message: %v", err)
	}

	if err := pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, gameState.GetUsername()),
		routing.PauseKey,
		pubsub.QueueTypeTransient,
		handlePause(gameState),
	); err != nil {
		log.Fatalf("subscribe direct message: %v", err)
	}

	for ; ; {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			if err := gameState.CommandSpawn(words); err != nil {
				fmt.Printf("%v\n", err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("%v\n", err)
				continue
			}

			if err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, gameState.GetUsername()),
				move,
			); err != nil {
				fmt.Printf("%v\n", err)
				continue
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				fmt.Printf("Too few arguments, expect 2, have %d\n", len(words))
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("cannot convert to a number: %v", err)
				continue
			}

			for range n {
				log := gamelogic.GetMaliciousLog()
				if err := publishGameLog(channel, gameState, routing.GameLog{
					CurrentTime: time.Now(),
					Message: log,
					Username: gameState.GetUsername(),
				}); err != nil {
					fmt.Println(err)
					continue
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			defer fmt.Println("RabbitMQ connection closed")
			return
		default:
			fmt.Printf("unknown command: %s\n", words[0])
		}
	}
}