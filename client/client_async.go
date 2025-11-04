package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "google.com/homework/proto/pb/agent"

	"google.golang.org/grpc"
)

func main() {
	// Подключаемся к gRPC серверу
	conn, err := grpc.Dial("localhost:1234", grpc.WithInsecure()) // ⚠️ в проде TLS
	if err != nil {
		log.Fatalf("не удалось подключиться: %v", err)
	}
	defer conn.Close()

	client := pb.NewMongoLoaderClient(conn)

	// 1. Запускаем асинхронную загрузку
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	asyncResp, err := client.AsyncLoadRange(ctx, &pb.LoadRequest{
		StartDate: "2012-02-01",
		EndDate:   "2012-02-03",
	})
	if err != nil {
		log.Fatalf("AsyncLoadRange error: %v", err)
	}
	fmt.Printf("Запущена задача: %s\n", asyncResp.TaskId)

	// 2. Опрос статуса задачи
	for {
		statusResp, err := client.GetTaskStatus(context.Background(), &pb.TaskStatusRequest{
			TaskId: asyncResp.TaskId,
		})
		if err != nil {
			log.Fatalf("GetTaskStatus error: %v", err)
		}

		fmt.Printf("Статус задачи %s: %s (inserted=%d/%d)\n",
			statusResp.TaskId,
			statusResp.Status,
			statusResp.Inserted,
			statusResp.Total,
		)

		if statusResp.Status == "completed" || statusResp.Status == "failed" {
			if statusResp.Error != "" {
				fmt.Printf("Ошибка: %s\n", statusResp.Error)
			}
			break
		}

		time.Sleep(2 * time.Second)
	}
}
