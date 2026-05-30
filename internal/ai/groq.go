package ai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"notify/internal/model"
	"os"

	"github.com/go-playground/validator/v10"
)

type Groq struct {
	Request   *GroqRequest
	Response  *GroqResponse
	Validator *validator.Validate
}

type GroqRequest struct {
	Model  string   `json:"model" validate:"required"`
	Inputs []*Input `json:"messages" validate:"required,min=1,dive,required"`
}

type GroqResponse struct {
	Id      string    `json:"id" validate:"required"`
	Choices []*Choice `json:"choices" validate:"required,min=1,dive,required"`
}

type Choice struct {
	Index        int      `json:"index"`
	Message      *Message `json:"message" validate:"required"`
	FinishReason string   `json:"finish_reason" validate:"required"`
}

type Message struct {
	Role    string `json:"role" validate:"required,oneof=user assistant system"`
	Content string `json:"content" validate:"required"`
	Refusal string `json:"refusal"`
}

type Input struct {
	Role    string `json:"role" validate:"required,oneof=user assistant system"` //either can be assistant, user or system
	Content string `json:"content" validate:"required"`
}

func (groq *Groq) Classify(body []byte) *model.NotifyDocument {
	doc := &model.NotifyDocument{}
	if err := json.Unmarshal(body, doc); err != nil {
		log.Fatal("The unmarshalling of the groq input failed")
	}
	firstInput := &Input{
		Role: "system",
		Content: fmt.Sprintf(`You are a severity classification agent.
Given an event with a source, message code, and description,
classify its severity as exactly one of: HIGH, MEDIUM, LOW.
Respond with only the single word. No explanation, no punctuation. HIGH - service failures, payment failures, security breaches, data loss, inventory low
MEDIUM - performance degradation, non critical errors, warnings  
LOW - informational events, successful operations, minor issues`),
	}
	inputs := []*Input{}
	inputs = append(inputs, firstInput)
	classifyInput := &Input{
		Role:    "user",
		Content: doc.MessageDescription,
	}
	inputs = append(inputs, classifyInput)
	groq.Request = &GroqRequest{
		Model:  os.Getenv("GROQ_MODEL"),
		Inputs: inputs,
	}
	if err := groq.Validator.Struct(groq.Request); err != nil {
		log.Fatal("groqRequest validation failed: ", err)
	}
	client := http.Client{}
	bodyByte, err := json.Marshal(groq.Request)
	if err != nil {
		log.Fatal("Marshal failed for request body: ", err.Error())
	}
	httpReq, err := http.NewRequest(http.MethodPost, os.Getenv("GROQ_API_URL"), bytes.NewBuffer(bodyByte))
	if err != nil {
		log.Fatal("Request creation failed: ", err.Error())
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", os.Getenv("GROQ_API_KEY")))
	res, err := client.Do(httpReq)
	if err != nil {
		log.Fatal("Response fetching failed: ", err.Error())
	}
	b := res.Body

	defer res.Body.Close()
	byteResponse, err := io.ReadAll(b)
	log.Println("Response from groq : ", string(byteResponse))
	if err != nil {
		log.Fatal("ReadAll method failed: ", err.Error())
	}
	groq.Response = &GroqResponse{}
	err = json.Unmarshal(byteResponse, groq.Response)
	if err != nil {
		log.Fatal("UnMarshal failed for response: ", err.Error())
	}
	if len(groq.Response.Choices) > 0 {
		doc.Severity = groq.Response.Choices[0].Message.Content

	}
	return doc
}
