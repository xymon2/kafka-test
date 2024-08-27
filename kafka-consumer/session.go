package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type Consumer struct {
	startOffsets   map[int32]int64 // 파티션별로 시작 오프셋을 지정
	kafkaConsumerG sarama.ConsumerGroup
}

func (consumer *Consumer) Setup(sess sarama.ConsumerGroupSession) error {
	fmt.Println("setup")
	//for partition, offset := range consumer.startOffsets {
	//	sess.ResetOffset("godot", partition, offset, "")
	//	fmt.Printf("Resetting offset for partition %d to %d\n", partition, offset)
	//}
	//a := sess.Claims()
	//fmt.Println(a)
	//sess.MarkOffset("godot", 0, 16, "")
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("cleanup")
	return nil
}

func (consumer *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("claim - start - topic : %s - partition : %d - initOffset - %d\n", claim.Topic(), claim.Partition(), claim.InitialOffset())
	for partition, offset := range consumer.startOffsets {
		sess.ResetOffset("godot", partition, offset, "")
		fmt.Printf("Resetting offset for partition %d to %d\n", partition, offset)
	}

	for msg := range claim.Messages() {
		fmt.Printf("Message claimed: partiton = %d, offset = %d\n", msg.Partition, msg.Offset)
		time.Sleep(1 * time.Second)
		sess.MarkMessage(msg, "")

		if claim.Partition() == 0 && msg.Offset < 7 {
			fmt.Println("mark offset")
			//sess.MarkOffset("godot", 0, msg.Offset+1, "")
		}
	}

	sess.Commit()
	fmt.Println("claim done")
	return nil
}

func main() {

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Return.Errors = true
	config.ClientID = "id"

	// Kafka 브로커 주소
	brokers := []string{"localhost:9092"}
	group := "test132334123454242"
	topics := []string{"godot"}

	startOffsets := map[int32]int64{0: 16}

	consumer := &Consumer{startOffsets: startOffsets}
	//consumer := &Consumer{}

	//ctx := context.Background()

	client, _ := sarama.NewClient(brokers, config)
	stamp := time.Date(2024, 8, 27, 15, 27, 36, 0, time.Local).UnixMilli()
	fmt.Println(stamp)
	ofset, err := client.GetOffset(topics[0], 0, stamp)
	fmt.Println(ofset)
	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, config)

	if err != nil {
		log.Fatalf("failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	//pa := map[string][]int32{
	//	"godot": {0, 1},
	//}

	consumer.kafkaConsumerG = consumerGroup
	//go func() {
	//	//for {
	//
	//	err := consumerGroup.Consume(ctx, topics, consumer)
	//	if err != nil {
	//		log.Printf("Error consuming: %v", err)
	//	}
	//
	//	if ctx.Err() != nil {
	//		return
	//	}
	//
	//	//}
	//}()

	<-sigchan
	fmt.Println("Shutdown signal received, exiting...")

}

//func createNewConsumerGroup(
//	ctx context.Context,
//	brokers []string,
//	consumerGroupName string,
//	clientID string,
//	startOffsets map[int32]int64,
//) (sarama.ConsumerGroup, error) {
//	config := sarama.NewConfig()
//	config.Version = sarama.V2_8_0_0
//	config.Consumer.Offsets.Initial = sarama.OffsetOldest
//	config.Consumer.Offsets.AutoCommit.Enable = false
//	config.ClientID = clientID
//
//	consumer := &Consumer{startOffsets: startOffsets}
//
//	consumerGroup, err := sarama.NewConsumerGroup(brokers, consumerGroupName, config)
//
//}
