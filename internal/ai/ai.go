package ai

import (
	"notify/internal/model"
)

type AIClassifier interface {
	Classify(body []byte) *model.NotifyDocument
}
