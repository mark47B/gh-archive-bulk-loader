package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
	"github.com/mark47B/gh-archive-bulk-loader/app/infrastructure/metrics"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

type MongoLoadRepo struct {
	coll *mongo.Collection
}

func NewMongoLoadRepo(collection *mongo.Collection) (repository.EventLoaderRepository, error) {
	return &MongoLoadRepo{coll: collection}, nil
}

func (r *MongoLoadRepo) InsertMany(ctx context.Context, docs []interface{}) (int64, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	start := time.Now()

	res, err := r.coll.InsertMany(ctx, docs)
	if err != nil {
		metrics.ErrorsTotal.WithLabelValues("insert").Inc()
		return 0, fmt.Errorf("error in InsertMany: %w", err)
	}
	elapsed := time.Since(start).Seconds()
	metrics.WriteDuration.Observe(elapsed)

	var inserted int64
	if res != nil {
		metrics.ErrorsTotal.WithLabelValues("insert").Inc()
		inserted = int64(len(res.InsertedIDs))
	}

	return inserted, nil
}
