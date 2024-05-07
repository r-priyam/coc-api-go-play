package main

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func MongoClient(uri string) *mongo.Client {
	client, err := mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(uri).SetServerAPIOptions(options.ServerAPI(options.ServerAPIVersion1)),
	)
	if err != nil {
		log.Fatal("Failed to connect to MongoDB")
	}

	return client
}
