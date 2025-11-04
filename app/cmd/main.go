package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mark47B/gh-archive-bulk-loader/app/config"
	"github.com/mark47B/gh-archive-bulk-loader/app/infrastructure/metrics"
	"github.com/mark47B/gh-archive-bulk-loader/app/infrastructure/storage"
	etcdStorage "github.com/mark47B/gh-archive-bulk-loader/app/infrastructure/storage/etcd"
	"github.com/mark47B/gh-archive-bulk-loader/app/infrastructure/storage/mongodb"
	redisStorage "github.com/mark47B/gh-archive-bulk-loader/app/infrastructure/storage/redis"
	"github.com/mark47B/gh-archive-bulk-loader/app/infrastructure/transport"
	"github.com/mark47B/gh-archive-bulk-loader/app/usecase"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	if len(os.Args) != 6 {
		log.Fatal("Usage: one-server <mongoURI> <dbName> <collName> <redisAddr> <redisDB>")
	}

	cfg := config.Config{
		MongoURI: os.Args[1],
		DBName:   os.Args[2],
		CollName: os.Args[3],

		RedisAddr: os.Args[4],
		RedisDB:   mustAtoi(os.Args[5]),
	}
	replicaLeadercfg := config.ReplicaLeaderConfig{
		LeaderKey:   "agent_leader",
		ReplicaID:   os.Getenv("REPLICA_ID"),
		LeaderLease: mustAtoi(os.Getenv("LEADER_LEASE")),
	}
	mainCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Infra
	mongoClient, err := mongodb.GetMongodbClient(cfg.MongoURI)
	if err != nil {
		log.Fatalf("failed to init mongo repo: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = mongoClient.Disconnect(ctx)
	}()

	eventCollection := mongoClient.Database(cfg.DBName).Collection(cfg.CollName)

	elr, err := mongodb.NewMongoLoadRepo(eventCollection)
	if err != nil {
		log.Fatalf("error in mongo load storage creation: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		DB:   cfg.RedisDB,
	})
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Redis connect error: %v", err)
	}
	qc := redisStorage.NewRedisQueryCache(redisClient)

	// replicaLeaderStore := redisStorage.NewRedisReplicaLeader(redisClient)
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"etcd:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("err connect to etcd: %v", err)
	}
	defer etcdClient.Close()
	replicaLeaderStore := etcdStorage.NewETCDReplicaLeader(etcdClient)

	downloader := storage.NewGHArchiveDownloader(5 * time.Minute)

	er := mongodb.NewMongoUserQueryRepo(eventCollection)

	atr := mongodb.NewMongoAsycnTaskRepo(mongoClient.Database(cfg.DBName))

	// UseCases
	ls := usecase.LoaderService{
		EventloaderRepo: elr,
		Downloader:      downloader,
		QueryCache:      qc,
	}
	qs := usecase.QueryService{
		QueryCache: qc,
		EventRepo:  er,
	}

	leaderService := usecase.NewReplicaLeaderService(replicaLeaderStore,
		replicaLeadercfg.LeaderKey,
		replicaLeadercfg.ReplicaID,
		time.Duration(replicaLeadercfg.LeaderLease)*time.Second)
	als := usecase.NewAsyncLoaderService(atr, elr, downloader, qc, &leaderService)

	// Transport
	grpcServer := transport.NewgRPCServer(ls, qs, &als)

	// Metrics
	go metrics.StartMetricsServer()

	// gRPC
	go func() {
		if err := transport.StartgRPCServer(mainCtx, grpcServer); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	// Replica Leader
	leaderService.RunLeaderElection(mainCtx)

	// Run background
	go als.StartBackground(mainCtx)

	// gracefull shotdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-mainCtx.Done():
		log.Printf("shutting down ctx done...")
		stop()
		als.GracefulShutdown()
		leaderService.GracefulShutdown()
	case sig := <-sigCh:
		log.Printf("caught signal: %v, shutting down gracefully...", sig)
		stop()
		als.GracefulShutdown()
		leaderService.GracefulShutdown()
	}

	<-time.After(3 * time.Second)
	log.Println("server stopped")
}

func mustAtoi(s string) int {
	val, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("Invalid Atoi string: %s -- %w", s, err)
	}
	return val
}
