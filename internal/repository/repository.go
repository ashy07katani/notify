package repository

import (
	"context"
	"log"
	"notify/internal/model"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type Repository struct {
	Notify       *mongo.Collection
	Subscription *mongo.Collection
}

func NewNotifyRepo() *Repository {
	client := InitDB()
	db := client.Database(os.Getenv("MONGO_DB"))
	notify := db.Collection("notify")
	subscription := db.Collection("subscription")
	return &Repository{
		Notify:       notify,
		Subscription: subscription,
	}
}

func InitDB() *mongo.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(os.Getenv("MONGO_URI")))
	if err != nil {
		panic("failed to initialize mongo client")
	}
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		log.Panic(err)
	}
	return client
}

func (repo *Repository) SaveNotify(ctx context.Context, doc *model.NotifyDocument) (*mongo.InsertOneResult, error) {
	res, err := repo.Notify.InsertOne(ctx, doc)
	return res, err
}
