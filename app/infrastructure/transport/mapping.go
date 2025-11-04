package transport

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mark47B/gh-archive-bulk-loader/app/domain/entity"
	pb "github.com/mark47B/gh-archive-bulk-loader/proto/pb/agent"
)

func toLoadRangeRequest(req *pb.LoadRequest) (*entity.LoadRangeRequest, error) {
	start, err := time.Parse("2006-01-02", req.StartDate)
	if err != nil {
		return nil, fmt.Errorf("bad start_date: %w", err)
	}
	end, err := time.Parse("2006-01-02", req.EndDate)
	if err != nil {
		return nil, fmt.Errorf("bad end_date: %w", err)
	}

	lrr := entity.LoadRangeRequest{StartDate: start, EndDate: end}
	return &lrr, nil
}

// UserQuery
func toUserQueryRequest(req *pb.UserQuery) *entity.UserQuery {
	return &entity.UserQuery{Username: req.Username}
}

func toUserQueryResponseDTO(resp *entity.UserQueryResponse) *pb.UserQueryResponse {
	out := &pb.UserQueryResponse{}

	// Events
	for _, e := range resp.Events {
		out.Events = append(out.Events, &pb.EventCount{
			Name:  e.Name,
			Count: e.Count,
		})
	}

	// EventsByYm
	for _, ym := range resp.EventsByYm {
		ymDTO := &pb.YearMonthEvents{
			Year:  ym.Year,
			Month: ym.Month,
		}
		for _, ev := range ym.Events {
			ymDTO.Events = append(ymDTO.Events, &pb.EventCount{
				Name:  ev.Name,
				Count: ev.Count,
			})
		}
		out.EventsByYm = append(out.EventsByYm, ymDTO)
	}

	// PushedTo
	out.PushedTo = append(out.PushedTo, resp.PushedTo...)

	return out
}

// Async user query request
func toTaskJobs(req *pb.LoadRequest) (*entity.Task, []*entity.TaskJob, error) {
	start, err := time.Parse("2006-01-02", req.StartDate)
	if err != nil {
		return nil, nil, fmt.Errorf("bad start_date: %w", err)
	}
	end, err := time.Parse("2006-01-02", req.EndDate)
	if err != nil {
		return nil, nil, fmt.Errorf("bad end_date: %w", err)
	}

	// build date list and urls
	dates := buildDateList(start, end) // []string "YYYY-MM-DD"
	jobs := make([]*entity.TaskJob, 0, len(dates)*24)
	for _, d := range dates {
		urls := buildURLForDate(d) // []string (24 часа)
		for _, u := range urls {
			jobs = append(jobs, &entity.TaskJob{URL: u, Date: d})
		}
	}

	task := &entity.Task{
		ID:        uuid.NewString(),
		StartDate: start,
		EndDate:   end,
		Status:    entity.TaskStatusRunning,
		UrlsTotal: len(jobs),
		UrlsDone:  0,
		Inserted:  0,
	}
	return task, jobs, nil
}

func buildDateList(start, end time.Time) []string {
	var dates []string
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		dates = append(dates, d.Format("2006-01-02"))
	}
	return dates
}

func buildURLForDate(date string) []string {
	urls := make([]string, 0, 24)
	for h := 0; h < 24; h++ {
		url := fmt.Sprintf("https://data.gharchive.org/%s-%d.json.gz", date, h)
		urls = append(urls, url)
	}
	return urls
}
