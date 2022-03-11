package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"k8s.io/utils/pointer"
	"log"
	"sync/atomic"

)

type msgConsumerGroup struct{
	name string
}
var 		i  *int32

func init ()  {
	i = pointer.Int32Ptr(0)
	atomic.StoreInt32(i,0)
}
func (msgConsumerGroup) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (msgConsumerGroup) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h msgConsumerGroup) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	maxSize :=0
	for msg := range claim.Messages() {
		ii := atomic.AddInt32(i,1)
		fmt.Printf("%s Message topic:%q partition:%d offset:%d,key: %s, i: %d\n", h.name, msg.Topic, msg.Partition, msg.Offset,msg.Key,ii)
		// 查mysql去重
		//if check(msg) {
		//	// 插入mysql
		//	insertToMysql()
		//}
		// 标记，sarama会自动进行提交，默认间隔1秒
		if len(msg.Value) >maxSize{
			fmt.Printf("size: %d, i: %d\n",len(msg.Value),ii)
			maxSize = len(msg.Value)
		}
		sess.MarkMessage(msg, "")
	}
	fmt.Printf("Messages: %d",atomic.LoadInt32(i))
	return nil
}

func main()  {
	brokerAddrs := []string{"10.249.132.56:9094","10.249.132.31:9094","10.249.132.87:9094"} // sy
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer(brokerAddrs,config)
}

func listTopics(brokerAddrs []string,cfg *sarama.Config){
	admin, err := sarama.NewClusterAdmin(brokerAddrs, cfg)
	if err != nil {
		log.Fatal("Error while creating cluster admin: ", err.Error())
	}
	topics,err := admin.ListTopics()
	if err != nil {
		log.Fatal("Error while list topic: ", err.Error())
	}
	for key,value := range topics{
		fmt.Printf("\ntopics: %+v",key)
		fmt.Printf("NumPartitions: %v,ReplicationFactor:%d",value.NumPartitions,value.ReplicationFactor)
	}
}

func consumer(brokerAddrs []string,cfg *sarama.Config){
	cGroup, err := sarama.NewConsumerGroup(brokerAddrs,"test-group", cfg)
	if err != nil {
		panic(err)
	}
	for {
		err := cGroup.Consume(context.Background(), []string{"testAutoSyncOffset"}, msgConsumerGroup{})
		if err != nil {
			fmt.Println(err.Error())
			break
		}
	}
	_ = cGroup.Close()
}