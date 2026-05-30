package main

import (
	"log"
	"notify/internal/ai"
	notifykafka "notify/internal/kafka"
	"notify/internal/migrations"
	"notify/internal/repository"
	"notify/internal/worker"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

// func main() {
// 	migrations.CreateTopic("notify-classified")
// 	msg := make(chan *kafka.Message, 3)
// 	log.Println("Starting worker main")
// 	var ai ai.AIClassifier = &ai.Groq{Validator: validator.New()}
// 	kafkaWriter := notifykafka.InitKafkaWriter("notify-classified")
// 	repo := repository.NewNotifyRepo()
// 	w := worker.NewWorkerService(kafkaWriter, ai, repo)
// 	w.Orchestrate(msg)
// 	log.Println("Exiting main.go")
// }

func main() {
	godotenv.Load("local.env")

	migrations.CreateTopic(os.Getenv("KAFKA_TOPIC_SLACK"))
	migrations.CreateTopic(os.Getenv("KAFKA_TOPIC_EMAIL"))

	msg := make(chan *kafka.Message, 3)
	log.Println("Starting worker main")

	var ai ai.AIClassifier = &ai.Groq{Validator: validator.New()}
	slackWriter := notifykafka.InitKafkaWriter(os.Getenv("KAFKA_TOPIC_SLACK"))
	emailWriter := notifykafka.InitKafkaWriter(os.Getenv("KAFKA_TOPIC_EMAIL"))
	repo := repository.NewNotifyRepo()

	w := worker.NewWorkerService(slackWriter, emailWriter, ai, repo)
	w.Orchestrate(msg)
	log.Println("Exiting main.go")
}
