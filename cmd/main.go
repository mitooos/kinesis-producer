package main

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	producer "github.com/mitooos/kinesis-producer"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-west-2"),
	)
	if err != nil {
		// handle error
		log.Fatal(err)
	}

	client := kinesis.NewFromConfig(cfg)

	pr := producer.New(&producer.Config{
		StreamName:   "test",
		BacklogCount: 2000,
		Client:       client,
	})

	pr.Start()

	// Handle failures
	go func() {
		for r := range pr.NotifyFailures() {
			// r contains `Data`, `PartitionKey` and `Error()`
			log.Printf("detected put failure, %v", r)
		}
	}()

	go func() {
		for i := 0; i < 5; i++ {
			err := pr.Put([]byte("foo"), "bar")
			if err != nil {
				log.Printf("error producing, %v", err)
			}
		}
	}()

	time.Sleep(3 * time.Second)
	pr.Stop()
}
