package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

var index =1232
func Producer(url string){
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:   "first_topic",
		Balancer: &kafka.LeastBytes{},
	}

	errWriter:=  w.WriteMessages(context.Background(),
		kafka.Message{
			//Key:   []byte("Key-"+string(index)),
			Value: []byte(""+url),
		},
	)
	index=index+1
	if errWriter!=nil{
		log.Fatal("failed to write message:",errWriter)
	}
	if errClose := w.Close(); errClose!=nil {
		log.Fatal("failsed to close writer:",errClose)
	}
}
func StartKafka(){


	conf := kafka.ReaderConfig{
		Brokers: []string {"localhost:9092"},
		Topic : "first_topic",
		GroupID : "gi",
	}

	reader := kafka.NewReader(conf)

	for{
		m,err := reader.ReadMessage(context.Background())
		if err!=nil{
			fmt.Printf("Some error occured",err)
			continue
		}
		fmt.Println("Message is:",string((m.Value)))
	}
}