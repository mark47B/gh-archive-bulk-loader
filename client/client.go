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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 1. Подключаемся к серверу загрузки (:1234)
	connLoader, err := grpc.Dial("localhost:1234", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect to loader: %v", err)
	}
	defer connLoader.Close()
	loaderClient := pb.NewMongoLoaderClient(connLoader)

	fmt.Println("=== Step 1: LoadRange ===")
	loadResp, err := loaderClient.LoadRange(ctx, &pb.LoadRequest{
		StartDate: "2012-02-01",
		EndDate:   "2012-02-03",
	})
	if err != nil {
		log.Fatalf("LoadRange failed: %v", err)
	}
	fmt.Printf("Inserted %d records into MongoDB\n\n", loadResp.InsertedCount)

	// 2. Подключаемся к серверу запросов (:1235)
	connQuery, err := grpc.Dial("localhost:1234", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect to query server: %v", err)
	}
	defer connQuery.Close()
	queryClient := pb.NewMongoLoaderClient(connQuery)

	fmt.Println("=== Step 2: QueryUser ===")
	userResp, err := queryClient.QueryUser(ctx, &pb.UserQuery{
		Username: "dilekmuhammet",
	})
	if err != nil {
		log.Fatalf("QueryUser failed: %v", err)
	}

	// pretty-print ответа
	fmt.Println("Events (total):")
	for _, e := range userResp.Events {
		fmt.Printf("  %s: %d\n", e.Name, e.Count)
	}
	fmt.Println()

	fmt.Println("Events by Year-Month:")
	for _, ym := range userResp.EventsByYm {
		fmt.Printf("  %d-%02d:\n", ym.Year, ym.Month)
		for _, e := range ym.Events {
			fmt.Printf("    %s: %d\n", e.Name, e.Count)
		}
	}
	fmt.Println()

	fmt.Println("Pushed to repos:")
	for _, repo := range userResp.PushedTo {
		fmt.Printf("  %s\n", repo)
	}
}
