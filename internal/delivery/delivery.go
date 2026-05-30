package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"notify/internal/common"
	notifykafka "notify/internal/kafka"
	"notify/internal/model"
	"notify/internal/repository"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type Delivery interface {
	Deliver(msg chan *kafka.Message)
}
type DeliverSlack struct {
	repo *repository.Repository
}

func NewDeliverySlack(repo *repository.Repository) Delivery {
	return &DeliverSlack{
		repo: repo,
	}
}
func (w *DeliverSlack) Deliver(msg chan *kafka.Message) {
	stopper := make(chan struct{})
	//topic and group need to be added in this method parameter
	rawTopic := os.Getenv("KAFKA_TOPIC_SLACK")
	rawGroupId := os.Getenv("KAFKA_CONSUMER_GROUP_SLACK")
	go notifykafka.InitReader(msg, stopper, rawTopic, rawGroupId)
	go func() {
		for m := range msg {
			log.Println("message: ", string(m.Key), " value: ", string(m.Value))
			fetchedDoc := &model.NotifyDocument{}
			err := json.Unmarshal(m.Value, fetchedDoc)
			if err != nil {
				log.Fatalf("failed to unmarshal fetched Kafka doc")
			}
			//make a database query to fetch the webhook link
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			res := w.repo.Subscription.FindOne(ctx, bson.M{"source": fetchedDoc.Source, "severity": fetchedDoc.Severity})
			var subscription = &model.SubscriptionDocument{}
			err = res.Decode(subscription)
			if err != nil {
				log.Fatalf("failed to either fetch or decode the fetched document : ", err.Error())
			}
			// now I have the subscription details I can create the message body I want to send and send the message to the webhook
			url := subscription.WebhookURL
			input := map[string]string{"text": fmt.Sprintf("🚨 Severity %s Alert received:\n%s", fetchedDoc.Severity, fetchedDoc.MessageDescription)}
			byteBody, err := json.Marshal(input)
			if err != nil {
				log.Fatalf("Marshalling of slack payload failed %s", err.Error())
			}
			slackMsgUtil(url, bytes.NewBuffer(byteBody))
			//if the program hasn't terminated then it means slackmessage was sent successful
			//change the status of the document to delivered in the database
			updateCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			updatedResult, err := w.repo.Notify.UpdateOne(updateCtx, bson.M{"_id": fetchedDoc.ID}, bson.M{"$set": bson.M{"status": common.StatusDelivered}})
			if err != nil {
				log.Fatalf("failed to update the status and severity", err.Error())
			}
			if updatedResult.MatchedCount > 1 {
				log.Printf("Updated status successfully")
			} else {
				log.Printf("matched count %d %+v", updatedResult.MatchedCount)
			}

		}
	}()
	<-stopper
}
func slackMsgUtil(url string, body io.Reader) {
	client := http.Client{}
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		log.Fatalf("failed to create request for slack %s", err.Error())
	}
	res, err := client.Do(req)
	resBody := res.Body
	defer res.Body.Close()
	resByte, err := io.ReadAll(resBody)
	if err != nil {
		log.Fatalf("failed to process response for slack %s", err.Error())
	}
	log.Println("successfully got response: ", string(resByte))
}
