package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/mark47B/gh-archive-bulk-loader/proto/pb/agent"

	"github.com/mark47B/gh-archive-bulk-loader/app/infrastructure/metrics"
	"github.com/mark47B/gh-archive-bulk-loader/app/usecase"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type gRPCServer struct {
	pb.UnimplementedMongoLoaderServer
	ls  usecase.LoaderService
	qs  usecase.QueryService
	als *usecase.AsyncLoaderService
}

func NewgRPCServer(ls usecase.LoaderService, qs usecase.QueryService, als *usecase.AsyncLoaderService) *gRPCServer {
	return &gRPCServer{ls: ls, qs: qs, als: als}
}

func StartgRPCServer(ctx context.Context, server *gRPCServer) error {
	lis, err := net.Listen("tcp", ":1234")
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	defer lis.Close()

	s := grpc.NewServer()
	pb.RegisterMongoLoaderServer(s, server)
	reflection.Register(s)

	errCh := make(chan error, 1)

	go func() {
		log.Printf("gRPC server listening on %s", ":1234")
		if err := s.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Context canceled, shutting down gRPC gracefully...")
		s.GracefulStop()
	case err := <-errCh:
		log.Printf("gRPC server error: %v", err)
		return err
	}
	return nil
}

func (s *gRPCServer) LoadRange(ctx context.Context, req *pb.LoadRequest) (*pb.LoadResponse, error) {
	tStart := time.Now()

	lrr, err := toLoadRangeRequest(req)
	if err != nil {
		return nil, fmt.Errorf("mapping error: %w", err)
	}
	inserted, err := s.ls.LoadRange(ctx, lrr)
	if err != nil {
		return nil, fmt.Errorf("load range loader service error: %w", err)
	}

	elapsed := time.Since(tStart).Seconds()
	if elapsed > 0 {
		metrics.RecordsPerSecond.Set(float64(inserted) / elapsed)
	}
	return &pb.LoadResponse{InsertedCount: inserted}, nil
}

func (s *gRPCServer) QueryUser(ctx context.Context, req *pb.UserQuery) (*pb.UserQueryResponse, error) {
	if req.Username == "" {
		return nil, fmt.Errorf("empty username")
	}
	uqr := toUserQueryRequest(req)
	uqresp, err := s.qs.QueryUser(ctx, uqr)
	if err != nil {
		return nil, fmt.Errorf("err user query: %w", err)
	}

	return toUserQueryResponseDTO(uqresp), nil
}

func (s *gRPCServer) AsyncLoadRange(ctx context.Context, req *pb.LoadRequest) (*pb.AsyncLoadResponse, error) {

	if !s.als.ReplicaLeaderService.AmILeader() {
		leaderID, err := s.als.ReplicaLeaderService.WhoLeader(ctx)

		if !s.als.ReplicaLeaderService.AmILeader() {
			if err != nil {
				return nil, fmt.Errorf("err who leader: %w", err)
			}
			conn, err := grpc.Dial(leaderID, grpc.WithInsecure())
			if err != nil {
				return nil, err
			}
			defer conn.Close()

			client := pb.NewMongoLoaderClient(conn)
			return client.AsyncLoadRange(ctx, req)
		}
	}

	task, jobs, err := toTaskJobs(req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	task_id, err := s.als.CommitAndStartTask(ctx, task, jobs)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.AsyncLoadResponse{TaskId: task_id}, nil

}

func (s *gRPCServer) GetTaskStatus(ctx context.Context, req *pb.TaskStatusRequest) (*pb.TaskStatusResponse, error) {
	if !s.als.ReplicaLeaderService.AmILeader() {

		leaderAddr, err := s.als.ReplicaLeaderService.WhoLeader(ctx)

		if !s.als.ReplicaLeaderService.AmILeader() {

			st := status.New(codes.FailedPrecondition, "request should be sent to Leader")

			errInfo := &errdetails.ErrorInfo{
				Reason:   "REPLICA_NOT_LEADER",
				Domain:   "mongo-loader",
				Metadata: map[string]string{"leader_address": leaderAddr},
			}
			st, err = st.WithDetails(errInfo)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to create error details: %v", err)
			}
			return nil, st.Err()
		}
	}

	task, err := s.als.GetStatus(ctx, req.TaskId)
	if err != nil {
		return nil, fmt.Errorf("err get task status: %w", err)
	}

	startDateProto := timestamppb.New(task.StartDate)
	endDateProto := timestamppb.New(task.EndDate)
	createdAtProto := timestamppb.New(task.CreatedAt)
	updatedAtProto := timestamppb.New(task.UpdatedAt)

	return &pb.TaskStatusResponse{
		TaskId:    task.ID,
		Status:    string(task.Status),
		UrlsTotal: int64(task.UrlsTotal),
		UrlsDone:  int64(task.UrlsDone),
		Inserted:  task.Inserted,
		Error:     task.Error,
		StartDate: startDateProto,
		EndDate:   endDateProto,
		CreatedAt: createdAtProto,
		UpdatedAt: updatedAtProto,
	}, nil
}
