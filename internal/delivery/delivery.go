package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/smtp"
	"notify/internal/common"
	notifykafka "notify/internal/kafka"
	"notify/internal/model"
	"notify/internal/repository"
	"os"
	"strings"
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
type DeliverMail struct {
	repo *repository.Repository
}

func NewDeliverySlack(repo *repository.Repository) Delivery {
	return &DeliverSlack{
		repo: repo,
	}
}

func NewDeliveryMail(repo *repository.Repository) Delivery {
	return &DeliverMail{
		repo: repo,
	}
}

func (w *DeliverSlack) Deliver(msg chan *kafka.Message) {
	deliver(w.repo, "KAFKA_TOPIC_SLACK", "KAFKA_CONSUMER_GROUP_SLACK", msg, func(fetchedDoc *model.NotifyDocument, subscription *model.SubscriptionDocument) {
		input := map[string]string{"text": fmt.Sprintf("🚨 Severity %s Alert received:\n%s", fetchedDoc.Severity, fetchedDoc.MessageDescription)}
		byteBody, err := json.Marshal(input)
		if err != nil {
			log.Fatalf("Marshalling of slack payload failed %s", err.Error())
		}
		slackMsgUtil(subscription.WebhookURL, bytes.NewBuffer(byteBody))
	})
}

func (w *DeliverMail) Deliver(msg chan *kafka.Message) {
	log.Println("[mail] starting mail delivery consumer")
	deliver(w.repo, "KAFKA_TOPIC_EMAIL", "KAFKA_CONSUMER_GROUP_MAIL", msg, func(fetchedDoc *model.NotifyDocument, subscription *model.SubscriptionDocument) {
		to := subscription.EmailAddresses
		from := os.Getenv("SMTP_FROM")
		log.Printf("[mail] sending to=%v from=%s source=%s severity=%s", to, from, fetchedDoc.Source, fetchedDoc.Severity)
		input := fmt.Sprintf("🚨 Severity %s Alert received:\n%s", fetchedDoc.Severity, fetchedDoc.MessageDescription)
		body := []byte(fmt.Sprintf("To: %s", strings.Join(to, ", ")) + "\r\n" +
			fmt.Sprintf("Subject: 🚨INCIDENT [%s] : Notify Alert\r\n", fetchedDoc.Source) +
			"\r\n" +
			input + "\r\n")
		mailMsgUtil(body, from, to, os.Getenv("SMTP_PASSWORD"))
		log.Printf("[mail] sent successfully to=%v", to)
	})
}

func deliver(repo *repository.Repository, topicEnv, groupEnv string, msg chan *kafka.Message, send func(*model.NotifyDocument, *model.SubscriptionDocument)) {
	stopper := make(chan struct{})
	go notifykafka.InitReader(msg, stopper, os.Getenv(topicEnv), os.Getenv(groupEnv))
	go func() {
		for m := range msg {
			log.Println("message: ", string(m.Key), " value: ", string(m.Value))
			fetchedDoc := &model.NotifyDocument{}
			if err := json.Unmarshal(m.Value, fetchedDoc); err != nil {
				log.Fatalf("failed to unmarshal fetched Kafka doc")
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			res := repo.Subscription.FindOne(ctx, bson.M{"source": fetchedDoc.Source, "severity": fetchedDoc.Severity})
			var subscription = &model.SubscriptionDocument{}
			if err := res.Decode(subscription); err != nil {
				log.Fatalf("failed to either fetch or decode the fetched document : ", err.Error())
			}

			send(fetchedDoc, subscription)

			updateCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			updatedResult, err := repo.Notify.UpdateOne(updateCtx, bson.M{"_id": fetchedDoc.ID}, bson.M{"$set": bson.M{"status": common.StatusDelivered}})
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
	if err != nil {
		log.Fatalf("failed to send slack request: %s", err.Error())
	}
	defer res.Body.Close()
	resByte, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("failed to process response for slack %s", err.Error())
	}
	log.Println("successfully got response: ", string(resByte))
}

func mailMsgUtil(body []byte, from string, to []string, password string) {
	addr := fmt.Sprintf("%s:%s", os.Getenv("SMTP_HOST"), os.Getenv("SMTP_PORT"))
	log.Printf("[mail] dialing SMTP addr=%s", addr)
	auth := smtp.PlainAuth("", from, password, "smtp.gmail.com")
	err := smtp.SendMail(addr, auth, from, to, body)
	if err != nil {
		log.Printf("[mail] smtp.SendMail failed: %s", err.Error())
		log.Fatal(err)
	}
}
