package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
)

type mongoUserQueryRepo struct {
	coll *mongo.Collection
}

func NewMongoUserQueryRepo(collection *mongo.Collection) repository.UserQueryRepository {
	return &mongoUserQueryRepo{
		coll: collection,
	}
}

func (r *mongoUserQueryRepo) GetUserEvents(ctx context.Context, username string) ([]entity.EventCount, error) {
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"actor.login", username}}}},
		{{"$group", bson.D{{"_id", "$type"}, {"count", bson.D{{"$sum", 1}}}}}},
		{{"$project", bson.D{{"name", "$_id"}, {"count", 1}, {"_id", 0}}}},
	}

	cur, err := r.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate events: %w", err)
	}
	defer cur.Close(ctx)

	var events []entity.EventCount
	for cur.Next(ctx) {
		var doc struct {
			Name  string `bson:"name"`
			Count int64  `bson:"count"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		events = append(events, entity.EventCount{Name: doc.Name, Count: doc.Count})
	}
	return events, cur.Err()
}

func (r *mongoUserQueryRepo) GetUserEventsByYM(ctx context.Context, username string) ([]entity.YearMonthEvents, error) {
	eventsByYMPipeline := mongo.Pipeline{
		{{"$match", bson.D{{"actor.login", username}}}},
		{{"$addFields", bson.D{{"__created_at", bson.D{{"$toDate", "$created_at"}}}}}},
		{{"$group", bson.D{
			{"_id", bson.D{
				{"year", bson.D{{"$year", "$__created_at"}}},
				{"month", bson.D{{"$month", "$__created_at"}}},
				{"type", "$type"},
			}},
			{"count", bson.D{{"$sum", 1}}},
		}}},
		{{"$group", bson.D{
			{"_id", bson.D{
				{"year", "$_id.year"},
				{"month", "$_id.month"},
			}},
			{"events", bson.D{{"$push", bson.D{
				{"name", "$_id.type"},
				{"count", "$count"},
			}}}},
		}}},
		{{"$project", bson.D{
			{"year", "$_id.year"},
			{"month", "$_id.month"},
			{"events", 1},
			{"_id", 0},
		}}},
		{{"$sort", bson.D{{"year", 1}, {"month", 1}}}},
	}

	cur2, err := r.coll.Aggregate(ctx, eventsByYMPipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate events_by_ym: %w", err)
	}
	defer cur2.Close(ctx)

	var eventsByYM []entity.YearMonthEvents
	for cur2.Next(ctx) {
		var doc entity.YearMonthEvents
		if err := cur2.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode events_by_ym doc: %w", err)
		}
		eventsByYM = append(eventsByYM, doc)
	}
	if err := cur2.Err(); err != nil {
		return nil, fmt.Errorf("cursor events_by_ym err: %w", err)
	}
	return eventsByYM, nil
}

func (r *mongoUserQueryRepo) GetUserPushedTo(ctx context.Context, username string) ([]string, error) {
	pushedPipeline := mongo.Pipeline{
		{{"$match", bson.D{{"actor.login", username}, {"type", "PushEvent"}}}},
		{{"$group", bson.D{{"_id", nil}, {"repos", bson.D{{"$addToSet", "$repo.name"}}}}}},
		{{"$project", bson.D{{"repos", 1}, {"_id", 0}}}},
	}

	cur3, err := r.coll.Aggregate(ctx, pushedPipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate pushed_to: %w", err)
	}
	defer cur3.Close(ctx)
	var doc struct {
		Repos []string `bson:"repos"`
	}
	if cur3.Next(ctx) {

		if err := cur3.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode pushed doc: %w", err)
		}
	}
	if err := cur3.Err(); err != nil {
		return nil, fmt.Errorf("cursor pushed err: %w", err)
	}
	return doc.Repos, cur3.Err()
}
