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
	msg := make(chan *kafka.Message, 3)
	deliverySlack := delivery.NewDeliverySlack(repo)
	deliverySlack.Deliver(msg)
}
