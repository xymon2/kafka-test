package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/golang/protobuf/proto"

	ch_proto "kafka-test/ch-proto"
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()

	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = false
	saramaConfig.ClientID = "id-2"

	subscriberConfig := kafka.SubscriberConfig{
		Brokers:               []string{"localhost:9092"}, // Kafka broker address
		Unmarshaler:           kafka.DefaultMarshaler{},   // Default Unmarshaler
		ConsumerGroup:         "new-group",
		OverwriteSaramaConfig: saramaConfig,
	}

	subscriber, err := kafka.NewSubscriber(subscriberConfig, logger)
	if err != nil {
		log.Fatalf("could not create Kafka subscriber: %v", err)
	}

	const topicName = "godot"
	//const topicName = "cht.general.outbox.test"

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	messages, err := subscriber.Subscribe(ctx, topicName)
	if err != nil {
		log.Fatalf("could not subscribe to topic: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for msg := range messages {
			a, _ := subscriber.PartitionOffset(topicName)
			fmt.Println(a)
			log.Printf("Received message UUID : %s\n", msg.UUID)
			//Deserialize the Protobuf message
			var protoMsg ch_proto.ExampleMessage
			if err := proto.Unmarshal(msg.Payload, &protoMsg); err != nil {
				log.Printf("could not deserialize protobuf message: %v", err)
				msg.Nack()
				continue
			}

			fmt.Printf("Received Protobuf message: ID=%s, Content=%s\n", protoMsg.Id, protoMsg.Content)
			msg.Ack()

			time.Sleep(1 * time.Second)
			print("Acked\n")
		}
		fmt.Println("Stop consuming")
	}()

	go func() {
		<-sigchan
		fmt.Println("Caught signal: terminating\n")
		cancel()
	}()

	wg.Wait()

	err = subscriber.Close()
	fmt.Println(err)
	fmt.Println("Subscriber closed")
}
