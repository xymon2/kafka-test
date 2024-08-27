package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "godot"
	partition := 0

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: partition,
		// group id 명시해야 consumer groups가 생성됨.
		GroupID:  "test1213",
		MinBytes: 1e3,
		MaxBytes: 10e6,
	})

	//(*Reader).SetOffset will return an error when GroupID is set
	// (*Reader).Offset will always return -1 when GroupID is set
	err := reader.SetOffset(6)
	if err != nil {
		log.Println("Error setting offset: ", err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	doneChan := make(chan bool)

	go func() {
		fmt.Println("Consuming messages...")
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Context canceled, stopping consumer...")
				doneChan <- true
				return
			default:
				// explicit commit
				//m, err := reader.FetchMessage(ctx)
				//if err != nil {
				//	break
				//}
				//if err := reader.CommitMessages(ctx, m); err != nil {
				//	log.Fatal("failed to commit messages:", err)
				//}
				//

				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					fmt.Printf("Error while reading message: %v\n", err)
					continue
				}

				fmt.Printf("Message on partition %d at offset %d: %s\n", msg.Partition, msg.Offset, string(msg.Value))

				time.Sleep(1 * time.Second)

			}
		}
	}()

	<-sigchan
	cancel()
	fmt.Println("Shutdown signal received, exiting...")
	<-doneChan
	fmt.Println("Done")
}
