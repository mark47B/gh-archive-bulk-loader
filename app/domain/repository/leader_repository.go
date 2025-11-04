package repository

import (
	"context"
	"time"
)

type ReplicaLeaderRepository interface {
	WhoLeader(ctx context.Context, leaderKey string) (string, error)
	RenewLock(ctx context.Context, leaderKey string, lockHolder string, leaderLease time.Duration) error
	TryAcquireLeader(ctx context.Context, leaderKey, replicaID string, leaderLease time.Duration) (bool, error)
	ReleaseLeader(ctx context.Context, leaderKey string, lockHolder string) error
}
