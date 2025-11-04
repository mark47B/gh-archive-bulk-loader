package repository

import (
	"context"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
)

type AsyncTaskRepository interface {
	CreateTask(ctx context.Context, t *entity.Task, jobs []*entity.TaskJob) error
	ReserveJob(ctx context.Context, taskID string, workerID string) (*entity.TaskJob, error)
	MarkJobDone(ctx context.Context, jobID string, inserted int64) error
	MarkJobError(ctx context.Context, jobID string, err string) error
	GetTask(ctx context.Context, taskID string) (*entity.Task, error)
	GetRunningTasks(ctx context.Context) ([]*entity.Task, error)
	UpdateTask(ctx context.Context, taskID string, doneJobs int, inserted int) error
	FinalizeTaskIfComplete(ctx context.Context, taskID string) error
}
