package model

import (
	"notify/internal/common"
	"time"
)

type NotifyRequest struct {
	MessageCode        string     `json:"messageCode" binding:"required"`
	MessageDescription string     `json:"messageDescription" binding:"required"`
	Source             string     `json:"source" binding:"required"`
	SourceID           string     `json:"sourceID" binding:"required"`
	ValidTill          *time.Time `json:"validTill"`
}

type NotifyResponse struct {
	Success   bool      `json:"success"`
	Message   string    `json:"message"`
	EventID   string    `json:"eventID"`
	CreatedAt time.Time `json:"createdAt"`
}

type ErrorResponse struct {
	Success bool   `json:"sucsess"`
	Message string `json:"message"`
}

type NotifyDocument struct {
	ID                 string             `bson:"_id"`
	MessageCode        string             `bson:"messageCode"`
	MessageDescription string             `bson:"messageDescription"`
	Source             string             `bson:"source"`
	SourceID           string             `bson:"sourceID"`
	Status             common.EventStatus `bson:"status"`
	Severity           string             `bson:"severity"`
	CreatedAt          time.Time          `bson:"createdAt"`
	ValidTill          *time.Time         `bson:"validTill"`
}

type SubscriptionDocument struct {
	Id         string `bson:"_id"`
	Source     string `bson:"source"`
	Severity   string `bson:"severity"`
	Channel    string `bson:"channel"`
	WebhookURL string `bson:"webhookURL"`
}
