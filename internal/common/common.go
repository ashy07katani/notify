package common

type EventStatus string
type ClassiedCategory string

const (
	StatusSaved      EventStatus = "saved"
	StatusProcessing EventStatus = "processing"
	StatusClassified EventStatus = "classified"
	StatusDelivered  EventStatus = "delivered"
	StatusInvalid    EventStatus = "invalid"
)

const (
	//HIGH, MEDIUM, LOW.
	CategoryHigh   ClassiedCategory = "HIGH"
	CategoryMedium ClassiedCategory = "MEDIUM"
	CategoryLow    ClassiedCategory = "LOW"
)

/*

saved
2) processing
3) classified
4) delivered
5) Invalid
*/
