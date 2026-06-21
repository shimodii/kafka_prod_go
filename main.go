package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	x := 10

	writer := kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "orders",
	}
	defer writer.Close()

	rate := x * 10
	interval := time.Second / time.Duration(rate)

	i := 0
	for {
		i++
		msg := kafka.Message{
			Value: []byte(fmt.Sprintf("order-%d", i)),
		}

		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println("Error: ", err)
		}

		time.Sleep(interval)
	}
}
