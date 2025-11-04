package usecase

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
)

const (
	producerWorkers = 3
	consumerWorkers = 8
	batchSize       = 1500
)

type LoaderService struct {
	EventloaderRepo repository.EventLoaderRepository
	Downloader      repository.GHArchiveDownloader
	QueryCache      repository.QueryCacheRepository
}

func (ls *LoaderService) LoadRange(ctx context.Context, lrr *entity.LoadRangeRequest) (int64, error) {
	log.Printf("[LoaderService] start LoadRange: %v → %v", lrr.StartDate, lrr.EndDate)

	urls := buildURLs(lrr)
	log.Printf("[LoaderService] generated %d URLs", len(urls))

	docCh := make(chan map[string]any, 6000)
	urlCh := make(chan string, len(urls))

	var inserted int64
	var wg sync.WaitGroup

	var newUsernames sync.Map

	// Consumers
	for i := 0; i < consumerWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var batch []interface{}
			for doc := range docCh {
				username, ok := getUsername(doc)
				if ok {
					newUsernames.Store(username, struct{}{})
				} else {
					// log.Printf("[consumer %d] missing or invalid actor.login in doc:", consumerIdx)
				}
				batch = append(batch, doc)
				if len(batch) >= batchSize {
					ins, err := ls.EventloaderRepo.InsertMany(ctx, batch)
					if err != nil {
						log.Printf("[Consumer-%d] ERROR inserting batch: %v", id, err)
					} else {
						atomic.AddInt64(&inserted, ins)
						log.Printf("[Consumer-%d] inserted batch of %d (total: %d)", id, ins, atomic.LoadInt64(&inserted))
					}
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				ins, err := ls.EventloaderRepo.InsertMany(ctx, batch)
				if err != nil {
					log.Printf("[Consumer-%d] ERROR inserting final batch: %v", id, err)
				} else {
					atomic.AddInt64(&inserted, ins)
					log.Printf("[Consumer-%d] inserted final batch of %d (total: %d)", id, ins, atomic.LoadInt64(&inserted))
				}
			}
		}(i)
	}

	// Producers
	var prodWG sync.WaitGroup
	for i := 0; i < producerWorkers; i++ {
		prodWG.Add(1)
		go func(id int) {
			defer prodWG.Done()
			for url := range urlCh {
				log.Printf("[Producer-%d] downloading %s", id, url)
				err := ls.Downloader.FetchAndParse(ctx, url, docCh)
				if err != nil {
					log.Printf("[Producer-%d] ERROR downloading %s: %v", id, url, err)
					continue
				}
				log.Printf("[Producer-%d] finished %s", id, url)
			}
		}(i)
	}

	for _, u := range urls {
		urlCh <- u
	}
	close(urlCh)

	prodWG.Wait()
	close(docCh)
	wg.Wait()

	// cache invalidization
	if err := ls.QueryCache.UniqueCacheInvalidization(ctx, &newUsernames); err != nil {
		log.Printf("LoadRange cache invalidization error: %v", err)
	}

	log.Printf("[LoaderService] finished LoadRange: total inserted=%d", inserted)
	return inserted, nil
}

func buildURLs(lrr *entity.LoadRangeRequest) []string {
	var urls []string
	for d := lrr.StartDate; !d.After(lrr.EndDate); d = d.AddDate(0, 0, 1) {
		for h := 0; h < 24; h++ {
			urls = append(urls,
				fmt.Sprintf("https://data.gharchive.org/%s-%d.json.gz",
					d.Format("2006-01-02"), h))
		}
	}
	return urls
}
