package usecase

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
)

type ReplicaLeaderService struct {
	leaderRepo repository.ReplicaLeaderRepository

	leaderKey   string
	replicaID   string // host:port
	leaderLease time.Duration

	iAmLeader    atomic.Bool
	cancelKeeper context.CancelFunc
}

func NewReplicaLeaderService(
	leadRepo repository.ReplicaLeaderRepository,
	leaderKey,
	replicaID string,
	leaderLease time.Duration) ReplicaLeaderService {

	return ReplicaLeaderService{
		leaderRepo:   leadRepo,
		leaderKey:    leaderKey,
		replicaID:    replicaID,
		leaderLease:  leaderLease,
		iAmLeader:    atomic.Bool{},
		cancelKeeper: nil,
	}
}

func (s *ReplicaLeaderService) keepLeadership(ctx context.Context) {
	leaseTicker := time.NewTicker(s.leaderLease / 2)
	defer leaseTicker.Stop()
	for {
		select {
		case <-leaseTicker.C:
			if err := s.leaderRepo.RenewLock(ctx, s.leaderKey, s.replicaID, s.leaderLease); err != nil {
				s.GracefulShutdown()
				log.Printf("error while re-new leader lock: %v", err)
				return
			}
		case <-ctx.Done():
			s.GracefulShutdown()
			log.Printf("ctx done")
			return
		}

	}
}

func (s *ReplicaLeaderService) RunLeaderElection(ctx context.Context) error {
	ok, err := s.leaderRepo.TryAcquireLeader(ctx, s.leaderKey, s.replicaID, s.leaderLease)
	if err != nil {
		s.iAmLeader.Store(false)
		return fmt.Errorf("err while try acquire leader: %w", err)
	}
	if !ok {
		s.iAmLeader.Store(false)
		log.Printf("[%s] NOT LEADER", s.replicaID)
		return nil
	}

	if s.cancelKeeper != nil {
		s.cancelKeeper()
	}
	log.Printf("[%s] GET LEADERSHIP", s.replicaID)
	s.iAmLeader.Store(true)
	c, cancelKeeping := context.WithCancel(context.Background())
	s.cancelKeeper = func() {
		cancelKeeping()
		s.iAmLeader.Store(false)
	}

	go s.keepLeadership(c)

	return nil
}

func (s *ReplicaLeaderService) WhoLeader(ctx context.Context) (string, error) {
	leaderID, err := s.leaderRepo.WhoLeader(ctx, s.leaderKey)
	if err != nil {
		if errors.Is(err, entity.ErrNoLeader) {
			if err := s.RunLeaderElection(ctx); err != nil {
				return "", fmt.Errorf("run election err: %w", err)
			}
			if s.iAmLeader.Load() {
				// if leaderID, err = s.leaderRepo.WhoLeader(ctx, s.leaderKey); err != nil {
				// 	return "", fmt.Errorf("didn't approve lidership with who: %w", err)
				// }
				return leaderID, nil
			}
		}
		return "", fmt.Errorf("err getting who leader at service: %w", err)
	}
	return leaderID, nil
}

func (s *ReplicaLeaderService) AmILeader() bool {
	return s.iAmLeader.Load()
}

func (s *ReplicaLeaderService) GracefulShutdown() {
	s.cancelKeeper()
	s.iAmLeader.Store(false)
	releaseCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.leaderRepo.ReleaseLeader(releaseCtx, s.leaderKey, s.replicaID); err != nil {
		log.Printf("error release leader: %v", err)
	}
}
