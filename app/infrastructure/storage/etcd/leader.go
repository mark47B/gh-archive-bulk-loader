package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ETCDReplicaLeader struct {
	client  *clientv3.Client
	leaseID clientv3.LeaseID
}

func NewETCDReplicaLeader(cli *clientv3.Client) repository.ReplicaLeaderRepository {
	return &ETCDReplicaLeader{
		client: cli,
	}
}

func (s *ETCDReplicaLeader) TryAcquireLeader(ctx context.Context, leaderKey, replicaID string, leaderLease time.Duration) (bool, error) {
	lease, err := s.client.Grant(ctx, int64(leaderLease.Seconds()))
	if err != nil {
		return false, fmt.Errorf("err grant lease: %w", err)
	}

	txn := s.client.Txn(ctx)
	txn.If(clientv3.Compare(clientv3.CreateRevision(leaderKey), "=", 0)).
		Then(clientv3.OpPut(leaderKey, replicaID, clientv3.WithLease(lease.ID)))

	resp, err := txn.Commit()
	if err != nil {
		return false, fmt.Errorf("transaction commit err:%w", err)
	}
	if !resp.Succeeded {
		return false, nil
	}

	s.leaseID = lease.ID

	return resp.Succeeded, nil
}

func (s *ETCDReplicaLeader) WhoLeader(ctx context.Context, leaderKey string) (string, error) {
	resp, err := s.client.Get(ctx, leaderKey)
	if err != nil {
		return "", fmt.Errorf("etcd get value err: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", entity.ErrNoLeader
	}
	return string(resp.Kvs[0].Value), nil
}

func (s *ETCDReplicaLeader) RenewLock(ctx context.Context, leaderKey string, lockHolder string, leaderLease time.Duration) error {
	if s.leaseID == 0 {
		return fmt.Errorf("renew lock: no lease assigned (not a leader?)")
	}

	txn := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(leaderKey), "=", lockHolder)).
		Then(clientv3.OpPut(leaderKey, lockHolder, clientv3.WithLease(s.leaseID)))

	resp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("renew lock txn commit: %w", err)
	}

	if !resp.Succeeded {
		return fmt.Errorf("renew lock failed: leader changed (expected=%s)", lockHolder)
	}

	kaResp, err := s.client.KeepAliveOnce(ctx, s.leaseID)
	if err != nil {
		return fmt.Errorf("keepalive once failed for lease %d: %w", s.leaseID, err)
	}
	if kaResp == nil {
		return fmt.Errorf("keepalive once returned nil for lease %d", s.leaseID)
	}
	return nil
}
func (s *ETCDReplicaLeader) ReleaseLeader(ctx context.Context, leaderKey string, lockHolder string) error {
	txn := s.client.Txn(ctx)
	txn.If(clientv3.Compare(clientv3.Value(leaderKey), "=", lockHolder)).
		Then(clientv3.OpDelete(leaderKey))
	resp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("etcd transaction err: %w", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("release leader: not owner or key changed (key=%s, expected=%s)", leaderKey, lockHolder)
	}
	if s.leaseID != 0 {
		s.client.Revoke(ctx, s.leaseID)
		s.leaseID = 0
	}
	return nil
}
