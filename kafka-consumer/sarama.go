package main


import (
"fmt"
"log"
"os"
"os/signal"

"github.com/Shopify/sarama"
)

func main() {
	// Kafka 브로커 목록
	brokers := []string{"localhost:9092"}
	topic := "godot"
	groupID := "new-group"

	// Sarama 컨슈머 그룹 구성
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0 // 사용 중인 Kafka 버전에 맞추어 변경
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // 가장 오래된 오프셋부터 읽기 시작

	// Sarama 컨슈머 그룹 생성
	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	// OS 시그널 채널 설정
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Sarama 컨슈머 핸들러 구현
	handler := ConsumerGroupHandler{}

	// 컨슈머 루프
	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{topic}, &handler); err != nil {
				log.Fatalf("Error consuming: %v", err)
			}
		}
	}()

	// 신호 대기
	<-signals
	fmt.Println("Interrupt is detected, shutting down...")
}

// ConsumerGroupHandler 구조체 정의
type ConsumerGroupHandler struct{}

// Setup는 Sarama에서 컨슈머 그룹이 시작될 때 호출됩니다.
func (ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup은 Sarama에서 컨슈머 그룹이 종료될 때 호출됩니다.
func (ConsumerGroupHandler) Cleanup(sarama
