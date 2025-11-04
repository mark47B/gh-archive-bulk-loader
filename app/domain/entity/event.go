package entity

import "time"

type LoadRangeRequest struct {
	StartDate time.Time
	EndDate   time.Time
}

type UserQuery struct {
	Username string
}

type EventCount struct {
	Name  string `bson:"name"`
	Count int64  `bson:"count"`
}

type YearMonthEvents struct {
	Year   int32        `bson:"year"`
	Month  int32        `bson:"month"`
	Events []EventCount `bson:"events"`
}

type UserQueryResponse struct {
	Events     []EventCount      `bson:"events"`
	EventsByYm []YearMonthEvents `bson:"eventsByYM"`
	PushedTo   []string          `bson:"pushedTo"`
}
