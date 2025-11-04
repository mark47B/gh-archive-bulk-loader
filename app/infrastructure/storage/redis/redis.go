package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
	"github.com/mark47B/gh-archive-bulk-loader/app/infrastructure/metrics"
	"github.com/redis/go-redis/v9"
)

type RedisQueryCache struct {
	client *redis.Client
}

func NewRedisQueryCache(rdb *redis.Client) repository.QueryCacheRepository {
	return &RedisQueryCache{client: rdb}
}

func (r *RedisQueryCache) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		metrics.CacheMisses.Inc()
		return nil, nil
	}
	metrics.CacheHits.Inc()
	return val, nil
}

func (r *RedisQueryCache) Set(ctx context.Context, key string, value []byte, ttlSeconds int) error {
	return r.client.Set(ctx, key, value, time.Duration(ttlSeconds)*time.Second).Err()
}

func (r *RedisQueryCache) InvalidateByUsernames(ctx context.Context, usernames map[string]struct{}) error {
	if len(usernames) == 0 {
		return nil
	}
	keys := make([]string, len(usernames))
	i := 0
	for username := range usernames {
		keys[i] = "user_stats:" + username
		i++
	}
	return r.client.Del(ctx, keys...).Err()
}

func (r *RedisQueryCache) GetAllUsernames(ctx context.Context) (map[string]struct{}, error) {
	var usernames = make(map[string]struct{})
	var cursor uint64
	prefix := "user_stats:"
	pattern := prefix + "*"

	for {
		var keys []string
		var err error

		keys, cursor, err = r.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("err redis scan: %w", err)
		}
		for _, key := range keys {
			if strings.HasPrefix(key, prefix) {
				value := key[len(prefix):]
				usernames[value] = struct{}{}
			}
		}

		if cursor == 0 {
			break
		}
	}
	return usernames, nil
}

func (r *RedisQueryCache) UniqueCacheInvalidization(ctx context.Context, newUsernames *sync.Map) error {

	cachedUsernames, err := r.GetAllUsernames(ctx)
	if err != nil {
		return fmt.Errorf("err get all usernames from cache: %w", err)
	}
	usernames, err := syncMapToOrdinaryMap(newUsernames)
	if err != nil {
		return fmt.Errorf("err transform syncMap to ordinaryMap: %w", err)
	}
	invalidUsernames, err := setIntersection(cachedUsernames, usernames)
	if err != nil {
		return fmt.Errorf("err set intersection: %w", err)
	}
	if err := r.InvalidateByUsernames(ctx, invalidUsernames); err != nil {
		return fmt.Errorf("err cache invalidate by username: %w", err)
	}
	return nil
}

func setIntersection(set1, set2 map[string]struct{}) (result map[string]struct{}, err error) {
	var iteration, check map[string]struct{}

	result = make(map[string]struct{})

	if len(set1) < len(set2) {
		iteration, check = set1, set2
	} else {
		iteration, check = set2, set1
	}

	for item := range iteration {
		if _, exists := check[item]; exists {
			result[item] = struct{}{}
		}
	}
	return result, nil
}

func syncMapToOrdinaryMap(syncMap *sync.Map) (map[string]struct{}, error) {
	regularMap := make(map[string]struct{})
	var err error

	syncMap.Range(func(key, value interface{}) bool {
		keyStr, ok := key.(string)
		if !ok {
			err = fmt.Errorf("key isn't string: %v", key)
			return false
		}
		regularMap[keyStr] = struct{}{}
		return true
	})

	return regularMap, err
}
