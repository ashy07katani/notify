package worker

import (
	"context"
	"encoding/json"
	"log"
	"notify/internal/ai"
	"notify/internal/common"
	notifykafka "notify/internal/kafka"
	"notify/internal/repository"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type Worker struct {
	slackWriter *kafka.Writer
	emailWriter *kafka.Writer
	ai          ai.AIClassifier
	repo        *repository.Repository
}

func NewWorkerService(slackWriter *kafka.Writer, emailWriter *kafka.Writer, ai ai.AIClassifier, repo *repository.Repository) *Worker {
	return &Worker{
		slackWriter: slackWriter,
		emailWriter: emailWriter,
		ai:          ai,
		repo:        repo,
	}
}

//this method would be responsible

/*
Consume from notify-raw
Call ai.Classify() → get severity
Publish to notify-classified with severity added
Update MongoDB status to classified

*/

func (w *Worker) Orchestrate(msg chan *kafka.Message) {
	stopper := make(chan struct{})
	//topic and group need to be added in this method parameter
	rawTopic := os.Getenv("KAFKA_TOPIC_RAW")
	rawGroupId := os.Getenv("KAFKA_CONSUMER_GROUP")
	go notifykafka.InitReader(msg, stopper, rawTopic, rawGroupId)
	go func() {
		for m := range msg {
			log.Println("message: ", string(m.Key), " value: ", string(m.Value))
			classifedDoc := w.ai.Classify(m.Value)

			kafkaCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			classifedDoc.Status = common.StatusClassified
			byteMessage, err := json.Marshal(classifedDoc)
			if err != nil {
				log.Fatalf("failed to marshal classiedDoc")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			switch classifedDoc.Severity {
			case string(common.CategoryHigh):
				w.slackWriter.WriteMessages(kafkaCtx, kafka.Message{Key: []byte(classifedDoc.SourceID), Value: byteMessage})
			case string(common.CategoryMedium):
				w.emailWriter.WriteMessages(kafkaCtx, kafka.Message{Key: []byte(classifedDoc.SourceID), Value: byteMessage})
			case string(common.CategoryLow):
				log.Printf("[LOW] source=%s sourceID=%s messageCode=%s message=%s createdAt=%s",
					classifedDoc.Source,
					classifedDoc.SourceID,
					classifedDoc.MessageCode,
					classifedDoc.MessageDescription,
					classifedDoc.CreatedAt,
				)
			}

			updatedResult, err := w.repo.Notify.UpdateOne(ctx, bson.M{"_id": classifedDoc.ID}, bson.M{"$set": bson.M{"severity": classifedDoc.Severity, "status": classifedDoc.Status}})
			if err != nil {
				log.Fatalf("failed to update the status and severity", err.Error())
			}
			if updatedResult.MatchedCount > 1 && (classifedDoc.Severity == string(common.CategoryLow) || classifedDoc.Severity == string(common.CategoryMedium) || classifedDoc.Severity == string(common.CategoryHigh)) {
				log.Printf("Updated status successfully to classified and the event successfully classified")
			} else {
				log.Printf("matched count %d %+v", updatedResult.MatchedCount, classifedDoc)
			}
			log.Printf("Repsonse from classifier %s", classifedDoc.Severity)
		}
	}()
	<-stopper
}
