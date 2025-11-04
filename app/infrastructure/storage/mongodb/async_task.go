package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoAsycnTaskRepo struct {
	tasksCol *mongo.Collection
	jobsCol  *mongo.Collection
}

func NewMongoAsycnTaskRepo(db *mongo.Database) repository.AsyncTaskRepository {
	return &MongoAsycnTaskRepo{
		tasksCol: db.Collection("tasks"),
		jobsCol:  db.Collection("task_jobs"),
	}
}

func (m *MongoAsycnTaskRepo) CreateTask(ctx context.Context, t *entity.Task, jobs []*entity.TaskJob) error {
	now := time.Now().UTC()
	t.CreatedAt = now
	t.UpdatedAt = now

	if t.ID == "" {
		t.ID = uuid.NewString()
	}
	_, err := m.tasksCol.InsertOne(ctx, t)
	if err != nil {
		return err
	}

	docs := make([]interface{}, 0, len(jobs))
	for _, j := range jobs {
		j.ID = uuid.NewString()
		j.TaskID = t.ID
		j.Status = "pending"
		docs = append(docs, j)
	}
	if len(docs) > 0 {
		_, err = m.jobsCol.InsertMany(ctx, docs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MongoAsycnTaskRepo) ReserveJob(ctx context.Context, taskID string, workerID string) (*entity.TaskJob, error) {
	filter := bson.M{"task_id": taskID, "status": "pending"}
	update := bson.M{"$set": bson.M{"status": "in_progress", "worker": workerID, "started_at": time.Now().UTC()}}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var job entity.TaskJob
	err := m.jobsCol.FindOneAndUpdate(ctx, filter, update, opts).Decode(&job)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &job, nil
}

func (m *MongoAsycnTaskRepo) UpdateTask(ctx context.Context, taskID string, doneJobs int, inserted int) error {
	filter := bson.M{"_id": taskID}
	update := bson.M{"$set": bson.M{"urls_done": doneJobs, "updated_at": time.Now(), "inserted": inserted}}
	result, err := m.jobsCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("[task %s] update job error: %w", taskID, err)
	}

	if result.ModifiedCount == 0 {
		return fmt.Errorf("task with ID %s not found", taskID)
	}

	return nil
}

func (m *MongoAsycnTaskRepo) MarkJobDone(ctx context.Context, jobID string, inserted int64) error {
	now := time.Now().UTC()
	// mark job done and set inserted
	_, err := m.jobsCol.UpdateOne(ctx, bson.M{"_id": jobID}, bson.M{
		"$set": bson.M{"status": "done", "finished_at": now, "inserted": inserted},
	})
	if err != nil {
		return err
	}

	// Increment Task urls_done and inserted
	var job entity.TaskJob
	if err := m.jobsCol.FindOne(ctx, bson.M{"_id": jobID}).Decode(&job); err != nil {
		return err
	}
	_, err = m.tasksCol.UpdateOne(ctx, bson.M{"_id": job.TaskID}, bson.M{
		"$inc": bson.M{"urls_done": 1, "inserted": inserted},
		"$set": bson.M{"updated_at": now},
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *MongoAsycnTaskRepo) MarkJobError(ctx context.Context, jobID string, errMsg string) error {
	now := time.Now().UTC()
	_, err := m.jobsCol.UpdateOne(ctx, bson.M{"_id": jobID}, bson.M{
		"$set": bson.M{"status": "error", "finished_at": now, "error": errMsg},
	})
	return err
}

func (m *MongoAsycnTaskRepo) GetTask(ctx context.Context, taskID string) (*entity.Task, error) {
	var t entity.Task
	if err := m.tasksCol.FindOne(ctx, bson.M{"_id": taskID}).Decode(&t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (m *MongoAsycnTaskRepo) GetRunningTasks(ctx context.Context) ([]*entity.Task, error) {
	cur, err := m.tasksCol.Find(ctx, bson.M{"status": entity.TaskStatusRunning})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var res []*entity.Task
	for cur.Next(ctx) {
		var t entity.Task
		if err := cur.Decode(&t); err != nil {
			return nil, err
		}
		res = append(res, &t)
	}
	return res, nil
}

// FinalizeTaskIfComplete: set status=done if urls_done == urls_total
func (m *MongoAsycnTaskRepo) FinalizeTaskIfComplete(ctx context.Context, taskID string) error {
	var t entity.Task
	if err := m.tasksCol.FindOne(ctx, bson.M{"_id": taskID}).Decode(&t); err != nil {
		return err
	}
	if t.UrlsDone >= t.UrlsTotal {
		_, err := m.tasksCol.UpdateOne(ctx, bson.M{"_id": taskID}, bson.M{
			"$set": bson.M{"status": entity.TaskStatusDone, "updated_at": time.Now().UTC()},
		})
		return err
	}
	return nil
}
