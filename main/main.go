package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	// Start a fake server running locally.
	srv := pstest.NewServer()
	defer srv.Close()
	topicID := "test-topic"
	projectID := "test-project"
	// Connect to the server without using TLS.
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		fmt.Print("World!")
	}
	defer conn.Close()
	// Use the connection when creating a pubsub client.
	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCConn(conn))
	if err != nil {
		fmt.Print("World!")
	}
	defer client.Close()
	ct, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		fmt.Print("World!")
	}
	subscription, err := client.CreateSubscription(ctx, "test-subscription", pubsub.SubscriptionConfig{Topic: ct})
	if err != nil {
		fmt.Print("World!")
	}
	_ = ct.Publish(ctx, &pubsub.Message{
		Data: []byte("hello world"),
	})
	err = subscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		fmt.Println(m.Data)
		m.Ack() // Acknowledge that we've consumed the message.
		cancel()
	})
	if err != nil {
		fmt.Print("World!")
	}

}
