package mongodb

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	clientInstance    *mongo.Client
	clientInstanceErr error
	mongoOnce         sync.Once
)

func GetMongodbClient(uri string) (*mongo.Client, error) {
	mongoOnce.Do(func() {
		clientOpts := options.Client().ApplyURI(uri)
		client, err := mongo.Connect(clientOpts)
		if err != nil {
			clientInstanceErr = err
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Ping(ctx, nil); err != nil {
			_ = client.Disconnect(ctx)
			clientInstanceErr = err
			return
		}
		clientInstance = client
	})

	return clientInstance, clientInstanceErr
}
