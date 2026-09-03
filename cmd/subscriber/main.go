package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/magnusp/watermill-coordinator/pkg/coordinator"
)

func main() {
	coordinatorAddr := flag.String("addr", getEnv("COORDINATOR_ADDR", "localhost:50051"), "gRPC coordinator address")
	topic := flag.String("topic", getEnv("TOPIC", "events"), "Topic to subscribe to")
	group := flag.String("group", getEnv("GROUP", "subscriber-agent"), "Consumer group")
	flag.Parse()

	log.Printf("Starting CloudEvents Subscriber Agent (connecting to %s, topic=%s, group=%s)...", *coordinatorAddr, *topic, *group)

	handler := func(ctx context.Context, event cloudevents.Event) error {
		log.Printf("[EVENT RECEIVED] ID: %s | Type: %s | Source: %s | Subject: %s | Time: %s",
			event.ID(), event.Type(), event.Source(), event.Subject(), event.Time())

		var prettyData interface{}
		if err := json.Unmarshal(event.Data(), &prettyData); err == nil {
			prettyJSON, _ := json.MarshalIndent(prettyData, "  ", "  ")
			log.Printf("  Data:\n  %s", string(prettyJSON))
		} else {
			log.Printf("  Data (raw): %s", string(event.Data()))
		}
		return nil
	}

	agent := coordinator.NewSubscriberAgent(coordinator.SubscriberAgentConfig{
		CoordinatorAddr: *coordinatorAddr,
		Topic:           *topic,
		ConsumerGroup:   *group,
	}, handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := agent.Start(ctx); err != nil {
		log.Fatalf("Failed to start subscriber agent: %v", err)
	}
	defer agent.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down subscriber agent...")
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
