package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	sleepSeconds := 5 // sleep X seconds between bursts

	writer := kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "orders",
	}
	defer writer.Close()

	i := 0
	for {
		var wg sync.WaitGroup

		for j := 0; j < 100; j++ {
			wg.Add(1)
			i++
			go func(seq int) {
				defer wg.Done()
				err := writer.WriteMessages(context.Background(), kafka.Message{
					Value: []byte(fmt.Sprintf("message %d", seq)),
				})
				if err != nil {
					fmt.Println("error:", err)
				}
			}(i)
		}

		wg.Wait()
		fmt.Printf("sent 100 messages, sleeping %d seconds...\n", sleepSeconds)
		time.Sleep(time.Duration(sleepSeconds) * time.Second)
	}
}
