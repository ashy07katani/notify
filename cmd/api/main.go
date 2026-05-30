package main

import (
	"notify/internal/api"

	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load("local.env")
	api.Start()
}
