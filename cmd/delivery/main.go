package main

import (
	"log"
	"notify/internal/delivery"
	"notify/internal/repository"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func main() {
	godotenv.Load("local.env")
	log.Println("Starting delivery main method.")
	repo := repository.NewNotifyRepo()
	slackMsg := make(chan *kafka.Message, 3)
	mailMsg := make(chan *kafka.Message, 3)
	deliverySlack := delivery.NewDeliverySlack(repo)
	go deliverySlack.Deliver(slackMsg)
	deliveryMail := delivery.NewDeliveryMail(repo)
	deliveryMail.Deliver(mailMsg)
}
