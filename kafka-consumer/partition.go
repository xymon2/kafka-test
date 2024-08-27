package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "godot"
	partition := int32(0) // 특정 파티션을 지정
	//offset := int64(2)    // 시작하고 싶은 오프셋을 지정

	// Sarama 설정 생성
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Sarama Consumer 생성
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	// 파티션 컨슈머 생성 (특정 파티션과 오프셋 지정)
	//partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Error creating partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// 메시지 소비 루프
	for msg := range partitionConsumer.Messages() {
		fmt.Printf("Consumed message offset %d: %s\n", msg.Offset, string(msg.Value))
	}
}
