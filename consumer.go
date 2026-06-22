package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
		GroupID: "orders-service",
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("error:", err)
			continue
		}
		fmt.Printf("offset=%d value=%s\n", msg.Offset, string(msg.Value))
		time.Sleep(time.Second * 5)
	}
}
