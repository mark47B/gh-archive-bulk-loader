package entity

import "time"

type TaskStatus string

const (
	TaskStatusRunning TaskStatus = "running"
	TaskStatusDone    TaskStatus = "done"
	TaskStatusError   TaskStatus = "error"
)

type Task struct {
	ID        string     `bson:"_id"`
	StartDate time.Time  `bson:"start_date"`
	EndDate   time.Time  `bson:"end_date"`
	Status    TaskStatus `bson:"status"`
	UrlsTotal int        `bson:"urls_total"`
	UrlsDone  int        `bson:"urls_done"`
	Inserted  int64      `bson:"inserted"`
	Error     string     `bson:"error,omitempty"`
	CreatedAt time.Time  `bson:"created_at"`
	UpdatedAt time.Time  `bson:"updated_at"`
}

type TaskJob struct {
	ID         string    `bson:"_id,omitempty"`
	TaskID     string    `bson:"task_id"`
	URL        string    `bson:"url"`
	Date       string    `bson:"date"`
	Status     string    `bson:"status"`
	Worker     string    `bson:"worker,omitempty"`
	StartedAt  time.Time `bson:"started_at,omitempty"`
	FinishedAt time.Time `bson:"finished_at,omitempty"`
	Inserted   int64     `bson:"inserted,omitempty"`
	Error      string    `bson:"error,omitempty"`
}
