package repository

import (
	"context"
	"sync"
)

type QueryCacheRepository interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttlSeconds int) error
	GetAllUsernames(ctx context.Context) (map[string]struct{}, error)
	InvalidateByUsernames(ctx context.Context, usernames map[string]struct{}) error
	UniqueCacheInvalidization(ctx context.Context, newUsernames *sync.Map) error
}
