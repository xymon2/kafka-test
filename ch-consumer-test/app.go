package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"

	"kafka-test/ch-consumer-test/handler"
)

func main() {
	// Gin의 기본 라우터 설정
	r := gin.Default()

	consumerHandler := createConsumerHandler()
	// GET 메서드로 "/ping" 엔드포인트를 처리
	r.POST("/resume/:topic/:partition", func(c *gin.Context) {
		topic := c.Param("topic")

		partition, _ := strconv.ParseInt(c.Param("partition"), 10, 32)
		consumerHandler.Resume(map[string][]int32{topic: {int32(partition)}})

		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		ctx := context.Background()
		defer consumerHandler.Close()
		err := consumerHandler.Consume(ctx)
		if err != nil {
			fmt.Printf("Error consuming: %v", err)
		}
	}()

	go func() {
		r.Run()
	}()

	<-sigchan
	fmt.Println("Shutdown signal received, exiting...")
}

func createConsumerHandler() handler.ConsumerHandler {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Return.Errors = true
	config.ClientID = "id"

	brokers := []string{"localhost:9092"}
	group := "test132334123454242"
	topics := []string{"godot"}

	return handler.NewConsumerHandler(brokers, config, group, topics)
}
