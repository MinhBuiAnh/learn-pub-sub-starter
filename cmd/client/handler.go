package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlePause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.TypeAck
	}
}

func handleMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(mv gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Printf("> ")
		outcome := gs.HandleMove(mv)
		
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.TypeAck
		case gamelogic.MoveOutcomeMakeWar:
			if err := pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()),
				gamelogic.RecognitionOfWar{
					Attacker: mv.Player,
					Defender: gs.GetPlayerSnap(),
				},
			); err != nil {
				fmt.Println(err)
				return pubsub.TypeNackRequeue
			}
			return pubsub.TypeAck
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.TypeNackDiscard
		}

		return pubsub.TypeNackDiscard
	}
}

func handleWar(gs *gamelogic.GameState, publishCh *amqp.Channel) func(message gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(message gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(message)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.TypeNackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.TypeNackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			logMessage := fmt.Sprintf("%s won a war against %s", winner, loser)
			if err := publishGameLog(publishCh, gs, routing.GameLog{
				CurrentTime: time.Now(),
				Message: logMessage,
				Username: gs.GetUsername(),
			}); err != nil {
				fmt.Println(err)
				return pubsub.TypeNackRequeue
			}
			return pubsub.TypeAck
		case gamelogic.WarOutcomeYouWon:
			logMessage := fmt.Sprintf("%s won a war against %s", winner, loser)
			if err := publishGameLog(publishCh, gs, routing.GameLog{
				CurrentTime: time.Now(),
				Message: logMessage,
				Username: gs.GetUsername(),
			}); err != nil {
				fmt.Println(err)
				return pubsub.TypeNackRequeue
			}
			return pubsub.TypeAck
		case gamelogic.WarOutcomeDraw:
			logMessage := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			if err := publishGameLog(publishCh, gs, routing.GameLog{
				CurrentTime: time.Now(),
				Message: logMessage,
				Username: gs.GetUsername(),
			}); err != nil {
				fmt.Println(err)
				return pubsub.TypeNackRequeue
			}
			return pubsub.TypeAck
		}

		fmt.Println("error: unknown outcome")
		return pubsub.TypeNackDiscard
	}
}

func publishGameLog(publishCh *amqp.Channel, gs *gamelogic.GameState, gl routing.GameLog) error {
	return pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, gs.GetUsername()),
		gl,
	)
}