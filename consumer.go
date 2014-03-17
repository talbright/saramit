package main

import (
  "github.com/Shopify/sarama"
  "fmt"
  "time"
)

func main() {
	client, err := sarama.NewClient("sarama_client", []string{"localhost:9092"}, &sarama.ClientConfig{MetadataRetries: 1, WaitForElection: 250 * time.Millisecond})
	fmt.Println("> client",client)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	consumer, err := sarama.NewConsumer(client, "repl5", 0, "sarama_consumer_group", &sarama.ConsumerConfig{OffsetMethod: sarama.OffsetMethodOldest, MaxWaitTime: 5000})
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> consumer",consumer)
	}
	defer consumer.Close()

  leader, err := client.Leader("repl5",0)

  fmt.Println("> leader",leader)
	msgCount := 0
	for {
		select {
		case event := <-consumer.Events():
			if event.Err != nil {
				panic(event.Err)
			}
			msgCount += 1
			fmt.Println("> received message #",msgCount,"with event", event)
      // Causes an error in kafka 
      offsetCommitRequest := &sarama.OffsetCommitRequest{ConsumerGroup: "sarama_consumer_group"}
      offsetCommitRequest.AddBlock("repl5",int32(0),int64(msgCount),"metadata")
      response, err := leader.CommitOffset("repl5",offsetCommitRequest)
	    if err != nil {
		    panic(err)
	    } else {
        fmt.Println("> committed offset",response)
      }
		case <-time.After(5 * time.Second):
			fmt.Println("> timed out")
		}
	}
}

