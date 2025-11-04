package repository

import (
	"context"
)

type EventLoaderRepository interface {
	InsertMany(ctx context.Context, docs []interface{}) (inserted int64, err error)
}
