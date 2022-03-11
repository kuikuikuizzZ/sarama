package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"math/rand"
	"strconv"
	"time"
)

func main(){
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // 等待所有follower都回复ack，确保Kafka不会丢消息
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner // 对Key进行Hash，同样的Key每次都落到一个分区，这样消息是有序的
	// 使用同步producer，异步模式下有更高的性能，但是处理更复杂，这里建议先从简单的入手
	brokerAddrs := []string{"10.249.132.56:9094","10.249.132.31:9094","10.249.132.87:9094"} // sy
	producer, err := sarama.NewSyncProducer(brokerAddrs, config)
	defer func() {
		_ = producer.Close()
	}()
	if err != nil {
		panic(err.Error())
	}
	msgCount := 1000
	// 模拟4个消息
	for i := 0; i < msgCount; i++ {
		rand.Seed(int64(time.Now().Nanosecond()))
		bytes := make([]byte,20000)
		msg := &sarama.ProducerMessage{
			Topic: "testAutoSyncOffset",
			Value: sarama.ByteEncoder(bytes),
			Key:   sarama.StringEncoder(strconv.Itoa(rand.Int())),
		}
		t1 := time.Now().Nanosecond()
		partition, offset, err := producer.SendMessage(msg)
		t2 := time.Now().Nanosecond()
		if err == nil {
			fmt.Println("produce success, partition:", partition, ",offset:", offset, ",cost:", (t2-t1)/(1000*1000), " ms")
		} else {
			fmt.Println(err.Error())
		}
	}
}