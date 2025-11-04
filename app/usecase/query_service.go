package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
)

type QueryService struct {
	QueryCache repository.QueryCacheRepository
	EventRepo  repository.UserQueryRepository
}

func (qs *QueryService) QueryUser(ctx context.Context, user *entity.UserQuery) (*entity.UserQueryResponse, error) {
	log.Printf("[QueryService] start QueryUser: username=%s", user.Username)
	// Check cache

	cacheKey := "user_stats:" + user.Username

	userQueryBytes, err := qs.QueryCache.Get(ctx, cacheKey)
	if err != nil {
		return nil, fmt.Errorf("cache get err: %w", err)
	}
	if userQueryBytes != nil {
		restored := &entity.UserQueryResponse{}
		if err := json.Unmarshal(userQueryBytes, restored); err != nil {
			return nil, fmt.Errorf("unmarshal user query in bytes err: %w", err)
		}
		return restored, err
	}
	log.Printf("[QueryService] cache miss for %s", cacheKey)

	// Request to mongo
	events, err := qs.EventRepo.GetUserEvents(ctx, user.Username)
	if err != nil {
		return nil, fmt.Errorf("err get user events from event repo: %w", err)
	}
	eventsByYM, err := qs.EventRepo.GetUserEventsByYM(ctx, user.Username)
	if err != nil {
		return nil, fmt.Errorf("err get user events by ym from event repo: %w", err)
	}
	pushedTo, err := qs.EventRepo.GetUserPushedTo(ctx, user.Username)
	if err != nil {
		return nil, fmt.Errorf("err get user pushed to from event repo: %w", err)
	}
	uqr := &entity.UserQueryResponse{
		Events:     events,
		EventsByYm: eventsByYM,
		PushedTo:   pushedTo,
	}

	// Cache miss
	data, err := json.Marshal(uqr)
	if err != nil {
		return nil, fmt.Errorf("failed marshal user query response: %w", err)
	}
	err = qs.QueryCache.Set(ctx, cacheKey, data, 0)
	if err != nil {
		return nil, fmt.Errorf("failed save query to cache: %w", err)
	}

	log.Printf("[QueryService] finished QueryUser: username=%s", user.Username)
	return uqr, nil
}
