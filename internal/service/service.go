package service

import (
	"context"
	"encoding/json"
	"net/http"
	"notify/internal/common"
	notifykafka "notify/internal/kafka"
	"notify/internal/model"
	"notify/internal/repository"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Service struct {
	NotifyRepo  *repository.Repository
	KafkaWriter *kafka.Writer
}

func InitService() *Service {
	service := new(Service)
	service.NotifyRepo = repository.NewNotifyRepo()
	service.KafkaWriter = notifykafka.InitKafkaWriter(os.Getenv("KAFKA_TOPIC_RAW"))
	return service
}

func (s *Service) PostNotify(c *gin.Context) {
	req := new(model.NotifyRequest)
	err := c.ShouldBindBodyWithJSON(req)
	if err != nil {
		errResponse := model.ErrorResponse{
			Success: false,
			Message: err.Error(),
		}
		c.JSON(http.StatusBadRequest, errResponse)
		return
	}
	eventId := uuid.New()
	createdAt := time.Now().UTC()
	res := model.NotifyResponse{
		Success:   true,
		Message:   "Request is under processing",
		EventID:   eventId.String(),
		CreatedAt: createdAt,
	}
	doc := &model.NotifyDocument{
		ID:                 res.EventID,
		MessageCode:        req.MessageCode,
		MessageDescription: req.MessageDescription,
		Source:             req.Source,
		Status:             common.StatusSaved,
		SourceID:           req.SourceID,
		CreatedAt:          createdAt,
		ValidTill:          req.ValidTill,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err = s.NotifyRepo.SaveNotify(ctx, doc)
	byteMessage, err := json.Marshal(doc)
	if err != nil {
		errResponse := model.ErrorResponse{
			Success: false,
			Message: err.Error(),
		}
		c.JSON(http.StatusInternalServerError, errResponse)
		return
	}
	kafkaCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	s.KafkaWriter.WriteMessages(kafkaCtx, kafka.Message{Key: []byte(doc.SourceID), Value: byteMessage})
	if err != nil {
		errResponse := model.ErrorResponse{
			Success: false,
			Message: err.Error(),
		}
		c.JSON(http.StatusInternalServerError, errResponse)
		return
	}

	c.JSON(http.StatusAccepted, res)
}
