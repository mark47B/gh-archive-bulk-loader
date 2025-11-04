package usecase

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
)

type AsyncLoaderService struct {
	TaskRepo   repository.AsyncTaskRepository
	EventRepo  repository.EventLoaderRepository
	Downloader repository.GHArchiveDownloader
	QueryCache repository.QueryCacheRepository

	ReplicaLeaderService *ReplicaLeaderService

	// parameters
	ctxWorkers           context.Context
	cancelWorkers        context.CancelFunc
	producerWorkers      int
	consumersPerProducer int

	batchSize int

	wg sync.WaitGroup
}

func NewAsyncLoaderService(
	taskRepo repository.AsyncTaskRepository,
	evtRepo repository.EventLoaderRepository,
	dl repository.GHArchiveDownloader,
	qc repository.QueryCacheRepository,

	replicaLeaderService *ReplicaLeaderService) AsyncLoaderService {
	ctx, cancel := context.WithCancel(context.Background())
	return AsyncLoaderService{
		TaskRepo:             taskRepo,
		EventRepo:            evtRepo,
		Downloader:           dl,
		QueryCache:           qc,
		ReplicaLeaderService: replicaLeaderService,
		producerWorkers:      5,
		consumersPerProducer: 3,
		batchSize:            1500,
		ctxWorkers:           ctx,
		cancelWorkers:        cancel,
	}
}

func (ls *AsyncLoaderService) CommitAndStartTask(ctx context.Context, task *entity.Task, jobs []*entity.TaskJob) (string, error) {
	if err := ls.TaskRepo.CreateTask(ctx, task, jobs); err != nil {
		return "", fmt.Errorf("err while create task: %w", err)
	}

	// run task at background
	ls.launchTask(ls.ctxWorkers, task.ID)
	return task.ID, nil
}

func (ls *AsyncLoaderService) StartBackground(ctx context.Context) error {
	tasks, err := ls.TaskRepo.GetRunningTasks(ctx)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		ls.launchTask(ls.ctxWorkers, task.ID)
	}
	return nil
}

func (ls *AsyncLoaderService) launchTask(ctx context.Context, taskID string) {
	ls.wg.Add(1)
	go func() {
		defer ls.wg.Done()
		ls.runTask(ctx, taskID)
	}()
}

func (ls *AsyncLoaderService) runTask(ctx context.Context, taskID string) {
	log.Printf("[task %s] starting worker loop", taskID)

	var inserted atomic.Int64
	var doneJobs atomic.Int32

	var newUsernames sync.Map

	// producers
	var prodWg sync.WaitGroup
	for p := 0; p < ls.producerWorkers; p++ {
		prodWg.Add(1)
		go func(workerIdx int) {
			defer prodWg.Done()
			wid := fmt.Sprintf("%s-%d", taskID, workerIdx)
			for {
				select {
				case <-ctx.Done():
					log.Printf("[task %s][producer %s] ctx canceled, exiting producer", taskID, wid)
					return
				default:
				}
				// reserve job
				job, err := ls.TaskRepo.ReserveJob(ctx, taskID, wid)
				if err != nil {
					log.Printf("[task %s][producer %s] reserve error: %v", taskID, wid, err)
					// time.Sleep(time.Second)
					continue
				}
				if job == nil {
					return
				}

				log.Printf("[task %s][producer %s] processing url %s", taskID, wid, job.URL)

				// Создаем канал для получения результатов парсинга
				parseCh := make(chan map[string]any, ls.batchSize)
				var parseWg sync.WaitGroup
				var parseErr atomic.Value
				var docsProcessed int64

				// consumers
				for c := 0; c < ls.consumersPerProducer; c++ {
					parseWg.Add(1)
					go func(consumerIdx int) {
						defer parseWg.Done()
						batch := make([]interface{}, 0, ls.batchSize)

						for doc := range parseCh {
							if username, ok := getUsername(doc); ok {
								newUsernames.Store(username, struct{}{})
							} else {
								// log.Printf("[consumer %d] missing or invalid actor.login in doc:", consumerIdx)
							}
							batch = append(batch, doc)
							if len(batch) >= ls.batchSize {
								ins, err := ls.EventRepo.InsertMany(ctx, batch)
								if err != nil {
									parseErr.Store(err)
									return
								}
								atomic.AddInt64(&docsProcessed, int64(ins))
								batch = batch[:0]
							}
						}
						if len(batch) > 0 {
							ins, err := ls.EventRepo.InsertMany(ctx, batch)
							if err != nil {
								parseErr.Store(err)
								return
							}
							atomic.AddInt64(&docsProcessed, int64(ins))
						}
						inserted.Add(docsProcessed)
					}(c)
				}

				// Fetch and parse
				dlCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
				err = ls.Downloader.FetchAndParse(dlCtx, job.URL, parseCh)
				cancel()

				close(parseCh)
				parseWg.Wait()

				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						log.Printf("[task %s][producer %s] timeout for %s. error -- %v", taskID, wid, job.URL, err)
					} else if errors.Is(err, context.Canceled) {
						log.Printf("[task %s][producer %s] canceled %s", taskID, wid, job.URL)
					} else {
						log.Printf("[task %s][producer %s] download error for %s: %v", taskID, wid, job.URL, err)
					}
					_ = ls.TaskRepo.MarkJobError(ctx, job.ID, err.Error())
					continue
				}

				if v := parseErr.Load(); v != nil {
					if err, ok := v.(error); ok {
						log.Printf("[task %s][producer %s] insert error for %s: %v", taskID, wid, job.URL, err)
						markCtx, markCancel := context.WithTimeout(context.Background(), 10*time.Second)
						_ = ls.TaskRepo.MarkJobError(markCtx, job.ID, err.Error())
						markCancel()
						continue
					}
				}

				// Mark job done only after successful insert
				doneJobs.Add(1)
				markCtx, markCancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := ls.TaskRepo.MarkJobDone(markCtx, job.ID, inserted.Load()); err != nil {
					log.Printf("[task %s][worker %s] MarkJobDone error: %v", taskID, wid, err)
				}
				markCancel()
			}
		}(p)
	}

	prodWg.Wait()

	finalCtx, finalCancel := context.WithTimeout(context.Background(), 10*time.Second)
	// cache invalidization
	if err := ls.QueryCache.UniqueCacheInvalidization(finalCtx, &newUsernames); err != nil {
		log.Printf("[task %s] cache invalidization error: %v", taskID, err)
	} else {
		// Try to finalize task (set status done if completed)
		if err := ls.TaskRepo.FinalizeTaskIfComplete(finalCtx, taskID); err != nil {
			log.Printf("[task %s] finalize error: %v", taskID, err)
		}

		log.Printf("[task %s] finished all jobs = %d, inserted ~= %d", taskID, doneJobs.Load(), inserted.Load())
	}
	finalCancel()

}

func (ls *AsyncLoaderService) GetStatus(ctx context.Context, taskID string) (*entity.Task, error) {
	task, err := ls.TaskRepo.GetTask(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("error get task from repository")
	}
	return task, nil
}

func (ls *AsyncLoaderService) GracefulShutdown() {
	if ls.cancelWorkers != nil {
		ls.cancelWorkers()
	}
	ls.wg.Wait()
}

func getUsername(doc map[string]any) (string, bool) {
	actorRaw, ok := doc["actor"]
	if !ok || actorRaw == nil {
		return "", false
	}

	actor, ok := actorRaw.(map[string]any)
	if !ok {
		return "", false
	}

	loginRaw, ok := actor["login"]
	if !ok || loginRaw == nil {
		return "", false
	}

	login, ok := loginRaw.(string)
	if !ok || login == "" {
		return "", false
	}

	return login, true
}
