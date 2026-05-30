package ai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"notify/internal/model"
	"os"
)

type Ollama struct {
	Request  *OllamaRequest
	Response *OllamResponse
}

type OllamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type OllamResponse struct {
	Response string `json:"response"`
}

func (ollama *Ollama) Classify(body []byte) *model.NotifyDocument {
	doc := &model.NotifyDocument{}
	if err := json.Unmarshal(body, doc); err != nil {
		log.Fatal("The unmarshalling of the ollama input failed")
	}
	ollama.Request = &OllamaRequest{
		Model:  os.Getenv("OLLAMA_MODEL"),
		Prompt: fmt.Sprintf("classify this as critical/high/medium/low only respond with the word: %s", doc.MessageDescription),
		Stream: false,
	}

	reqByte, err := json.Marshal(ollama.Request)
	if err != nil {
		log.Fatal("The marshalling of the ollama paylaod failed")
	}
	resp, err := http.Post(os.Getenv("OLLAMA_URL"), "application/json", bytes.NewReader(reqByte))
	if err != nil {
		log.Fatal("Couldn't get response from ollama")
	}
	var res OllamResponse
	err = json.NewDecoder(resp.Body).Decode(ollama.Response)
	if err != nil {
		log.Fatal("cannot decode the response from ollama")
	}
	log.Println("response: ", res.Response)
	doc.Severity = res.Response
	return doc
}
