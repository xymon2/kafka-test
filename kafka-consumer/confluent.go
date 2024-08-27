package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers":    "localhost:9092",
		"group.id":             "test11231232",
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   false,
		"session.timeout.ms":   6000,
		"client.id":            "id-12",
		"enable.partition.eof": true,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Subscribe to topics
	topics := []string{"godot"}

	topicPartition := kafka.TopicPartition{
		Topic:     &topics[0],
		Partition: 0,
		Offset:    14,
	}

	// Setup signal catching for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topics: %v", err)
	}

	date := "2024-08-18 23:56:00"
	timestamp := convertTimeToTimestamp(date)

	times := []kafka.TopicPartition{
		{
			Topic:     &topics[0],
			Partition: 0,
			Offset:    kafka.Offset(timestamp),
		},
	}

	offsets, err := consumer.OffsetsForTimes(times, 1000)
	if err != nil {
		log.Fatalf("Failed to get offsets: %v", err)
	}
	fmt.Println("offsets : ", offsets)

	//topicPartition2 := kafka.TopicPartition{
	//	Topic:     &topics[0],
	//	Partition: 1,
	//	Offset:    21,
	//}

	err = consumer.Assign([]kafka.TopicPartition{topicPartition})
	if err != nil {
		log.Fatalf("Failed to assign partition: %v", err)
	}

	a, err := consumer.Assignment()
	if err != nil {
		log.Fatalf("Failed to get assignment: %v", err)
	}
	fmt.Println("assignment : ", a)

	ctx, cancel := context.WithCancel(context.Background())

	err = consumer.Pause([]kafka.TopicPartition{topicPartition})
	if err != nil {
		log.Fatalf("Failed to pause partition: %v", err)
	}
	doneChan := make(chan bool)

	go func() {
		fmt.Println("Consuming messages...")
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Ctx Done")
				doneChan <- true
				return

			default:
				ev := consumer.Poll(1000)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					fmt.Printf("Message on %d:\n%s\n", e.TopicPartition.Partition, e.TopicPartition.Offset)
					time.Sleep(1 * time.Second)
					_, err = consumer.CommitMessage(e)
					if err != nil {
						fmt.Println("commit msg error : ", err)
					}
				case kafka.PartitionEOF:
					fmt.Printf("Reached end of partition %v\n", e)
				default:
					fmt.Printf("Ignored %v\n", e)
				}

				//msg, err := consumer.ReadMessage(10 * time.Second) // -1은 무한 대기
				//if err != nil {
				//	fmt.Printf("Consumer error: %v (%v)\n", err, msg)
				//	return
				//}
				//fmt.Printf("Message on %d: %s\n", msg.TopicPartition.Partition, msg.TopicPartition.Offset)
				//
				//_, err = consumer.CommitMessage(msg)
				////_, err = consumer.Commit()
				//if err != nil {
				//	fmt.Println("commit error : ", err)
				//}
				//fmt.Println("commit done")
			}
		}
	}()

	<-sigchan
	cancel()
	fmt.Println("Shutdown signal received, exiting...")
	<-doneChan
	fmt.Println("Done")
}

func convertTimeToTimestamp(date string) int {
	format := "2006-01-02 15:04:05"

	t, err := time.Parse(format, date)
	if err != nil {
		fmt.Println(err)
	} else {
		return int(t.Unix()) * 1000
	}
	return 0
}
