package storage

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mark47B/gh-archive-bulk-loader/app/domain/repository"
	"github.com/mark47B/gh-archive-bulk-loader/app/infrastructure/metrics"
)

type GHArchiveDownloader struct {
	client *http.Client
}

func NewGHArchiveDownloader(timeout time.Duration) repository.GHArchiveDownloader {
	return &GHArchiveDownloader{
		client: &http.Client{Timeout: timeout},
	}
}

func (d *GHArchiveDownloader) FetchAndParse(ctx context.Context, url string, out chan<- map[string]any) error {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		metrics.ErrorsTotal.WithLabelValues("download").Inc()
		return fmt.Errorf("http request: %w", err)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		metrics.ErrorsTotal.WithLabelValues("download").Inc()
		return fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()
	dur := time.Since(start).Seconds()
	metrics.DownloadDuration.Observe(dur)

	if resp.StatusCode != http.StatusOK {
		metrics.ErrorsTotal.WithLabelValues("download").Inc()
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		metrics.ErrorsTotal.WithLabelValues("download").Inc()
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return err
		}
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gzr.Close()

	dec := json.NewDecoder(gzr)
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			metrics.ErrorsTotal.WithLabelValues("download").Inc()
			return fmt.Errorf("json decode: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- doc:
		}
	}
	return nil
}
