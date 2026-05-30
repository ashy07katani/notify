package notifykafka

import (
	"context"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

func InitKafkaWriter(topic string) *kafka.Writer {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{os.Getenv("KAFKA_BROKER")},
		Topic:    topic,
		Balancer: &kafka.Hash{},
	})
	return w
}

func InitReader(msg chan *kafka.Message, stopper chan struct{}, topic string, groupId string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_BROKER")},
		GroupID:  groupId,
		Topic:    topic,
		MaxBytes: 10e6, // 10MB
	})
	r.SetOffset(42)
	defer func() {
		close(msg)
		stopper <- struct{}{}
	}()
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println(err.Error())
			break
		}
		msg <- &m
		log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

/*
w := kafka.NewWriter(kafka.WriterConfig{
	Brokers: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
	Topic:   "topic-A",
	Balancer: &kafka.Hash{},
	Dialer:   dialer,
})
*/
