package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
	"github.com/redis/go-redis/v9"
)

type RedisReplicaLeader struct {
	client *redis.Client
}

func NewRedisReplicaLeader(c *redis.Client) repository.ReplicaLeaderRepository {
	return &RedisReplicaLeader{client: c}
}

func (rrl *RedisReplicaLeader) WhoLeader(ctx context.Context, leaderKey string) (string, error) {
	leader, err := rrl.client.Get(ctx, leaderKey).Result()
	switch {
	case err == redis.Nil:
		return "", entity.ErrNoLeader
	case err != nil:
		return "", fmt.Errorf("error getting leader key: %w", err)
	default:
		return leader, nil
	}
}

func (r *RedisReplicaLeader) ReleaseLeader(ctx context.Context, leaderKey string, holderID string) error {
	val, err := r.client.Get(ctx, leaderKey).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	if val == holderID {
		_, err = r.client.Del(ctx, leaderKey).Result()
		if err != nil {
			return err
		}
		log.Printf("[%s] has released the leadership", holderID)
	}
	return nil
}

func (rrl *RedisReplicaLeader) RenewLock(ctx context.Context, leaderKey string, lockHolder string, leaderLease time.Duration) error {
	val, err := rrl.client.Get(ctx, leaderKey).Result()
	if err != nil {
		if err != redis.Nil {
			return fmt.Errorf("redis get error: %w", err)
		}
		log.Printf("[%s] record expired", leaderKey)
		log.Printf("[%s] -> [%s] try acquire", leaderKey, lockHolder)
		ok, _ := rrl.TryAcquireLeader(ctx, leaderKey, lockHolder, leaderLease)
		if ok {
			return nil
		}
		return fmt.Errorf("not leader")

	}
	if val != lockHolder {
		log.Printf("[%s] lost leadership", lockHolder)
		return fmt.Errorf("not leader")
	}
	ok, err := rrl.client.Expire(ctx, leaderKey, leaderLease).Result()
	if err != nil {
		return fmt.Errorf("[%s] expire redis error: %w", lockHolder, err)
	}
	if !ok {
		return fmt.Errorf("[%s] failed to renew lease", lockHolder)
	}

	return nil
}

func (rrl *RedisReplicaLeader) TryAcquireLeader(ctx context.Context, leaderKey, replicaID string, leaderLease time.Duration) (bool, error) {
	ok, err := rrl.client.SetNX(ctx, leaderKey, replicaID, leaderLease).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx error: %w", err)
	}
	return ok, nil
}
