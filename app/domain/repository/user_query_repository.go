package repository

import (
	"context"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
)

type UserQueryRepository interface {
	GetUserEvents(ctx context.Context, username string) ([]entity.EventCount, error)
	GetUserEventsByYM(ctx context.Context, username string) ([]entity.YearMonthEvents, error)
	GetUserPushedTo(ctx context.Context, username string) ([]string, error)
}
