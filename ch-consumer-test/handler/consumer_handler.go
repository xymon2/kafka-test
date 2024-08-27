package handler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type ConsumerHandler interface {
	Setup(sess sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
	Consume(ctx context.Context) error
	Close()

	Pause(topicPartition map[string][]int32)
	Resume(topicPartition map[string][]int32)
}

type consumerHandler struct {
	topics         []string
	consumerGroup  sarama.ConsumerGroup
	consumerClient sarama.Client
}

func NewConsumerHandler(
	brokers []string,
	config *sarama.Config,
	groupID string,
	topics []string,
) ConsumerHandler {

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}

	return &consumerHandler{
		topics:         topics,
		consumerGroup:  consumerGroup,
		consumerClient: client,
	}
}

func (c *consumerHandler) Close() {
	if err := c.consumerGroup.Close(); err != nil {
		log.Fatalf("Error closing consumer group: %v", err)
	}
	if err := c.consumerClient.Close(); err != nil {
		log.Fatalf("Error closing client: %v", err)
	}
}

func (c *consumerHandler) Consume(ctx context.Context) error {
	return c.consumerGroup.Consume(ctx, c.topics, c)
}

func (c *consumerHandler) Setup(sess sarama.ConsumerGroupSession) error {

	return nil
}

func (c *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	objectHandler := NewHandler()
	fmt.Printf("claim - start - topic : %s - partition : %d - initOffset - %d\n", claim.Topic(), claim.Partition(), claim.InitialOffset())

	// initial pause part
	c.Pause(map[string][]int32{claim.Topic(): {claim.Partition()}})

	// reset offset part
	partitonConfig := ConfigSet[claim.Topic()][claim.Partition()]
	if partitonConfig.ResetOffset {
		offset, err := c.consumerClient.GetOffset(claim.Topic(), claim.Partition(), partitonConfig.OffsetTime.UnixMilli())
		if err != nil {
			fmt.Printf("Failed to get offset: %v\n", err)
			return err
		}
		fmt.Printf("Resetting offset for partition %d to %d\n", claim.Partition(), offset)
		sess.ResetOffset(claim.Topic(), claim.Partition(), offset, "")
	}

	// consume part
	for msg := range claim.Messages() {
		fmt.Printf("Message claimed: topic = %s, partiton = %d, offset = %d\n", msg.Topic, msg.Partition, msg.Offset)
		ctx, cancel := context.WithTimeout(sess.Context(), 30*time.Second)

		// idempotent part(or it can be used as retry logic)
		MessageCounter[fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)] += 1
		if MessageCounter[fmt.Sprintf("%s-%d", msg.Topic, msg.Partition)] > 1 {
			fmt.Printf("send DLT:  topic = %s, partiton = %d, offset = %d\n", msg.Topic, msg.Partition, msg.Offset)
			sess.MarkMessage(msg, "")
			cancel()
			continue
		}

		err := objectHandler.HandleObject(ctx, msg.Value)
		cancel()

		if err != nil {
			fmt.Printf("Error Occured:  topic = %s, partiton = %d, offset = %d\n err=%v\n", msg.Topic, msg.Partition, msg.Offset, err)
		} else {
			sess.MarkMessage(msg, "")
		}
	}

	return nil
}

func (c *consumerHandler) Pause(topicPartition map[string][]int32) {
	for topic, partitions := range topicPartition {
		fmt.Println("Pause topic: ", topic, " partitions: ", partitions)
	}

	c.consumerGroup.Pause(topicPartition)
}

func (c *consumerHandler) Resume(topicPartition map[string][]int32) {
	for topic, partitions := range topicPartition {
		fmt.Println("Resume topic: ", topic, " partitions: ", partitions)
	}
	c.consumerGroup.Resume(topicPartition)
}
