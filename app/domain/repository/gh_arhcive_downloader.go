package repository

import "context"

type GHArchiveDownloader interface {
	FetchAndParse(ctx context.Context, url string, out chan<- map[string]any) error
}
