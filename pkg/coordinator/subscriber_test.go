package coordinator

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	pb "github.com/magnusp/watermill-coordinator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestSubscriberAgent_CloudEvents_StructuredAndBinary(t *testing.T) {
	db, client, cleanup := setupTestGRPC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Publish a Structured CloudEvent
	ceStructured := cloudevents.NewEvent()
	ceStructured.SetID("ce-struct-101")
	ceStructured.SetSource("urn:service:publisher")
	ceStructured.SetType("io.service.order.created.v1")
	ceStructured.SetSubject("orders/order-101")
	_ = ceStructured.SetData(cloudevents.ApplicationJSON, map[string]string{
		"orderId": "order-101",
		"status":  "CREATED",
	})
	ceBytes, err := json.Marshal(ceStructured)
	if err != nil {
		t.Fatalf("marshal structured CE: %v", err)
	}

	_, err = client.Publish(ctx, &pb.PublishRequest{
		Topic:     "events_ce",
		MessageId: ceStructured.ID(),
		Payload:   ceBytes,
		Metadata: map[string]string{
			"content-type": "application/cloudevents+json",
		},
	})
	if err != nil {
		t.Fatalf("publish structured CE: %v", err)
	}

	// 2. Publish a Binary CloudEvent
	rawPayload := []byte(`{"orderId":"order-102","status":"CONFIRMED"}`)
	_, err = client.Publish(ctx, &pb.PublishRequest{
		Topic:     "events_ce",
		MessageId: "ce-bin-202",
		Payload:   rawPayload,
		Metadata: map[string]string{
			"ce-id":              "ce-bin-202",
			"ce-source":          "urn:service:order-processor",
			"ce-type":            "io.service.order.confirmed.v1",
			"ce-subject":         "orders/order-102",
			"ce-datacontenttype": "application/json",
		},
	})
	if err != nil {
		t.Fatalf("publish binary CE: %v", err)
	}

	// 3. Connect SubscriberAgent and verify both are received & parsed as CloudEvents
	receivedEvents := make(chan cloudevents.Event, 2)
	handler := func(ctx context.Context, e cloudevents.Event) error {
		receivedEvents <- e
		return nil
	}

	// Create in-memory gRPC server listener for subscriber
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	svc, err := NewService(db, ServiceOptions{})
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}
	defer func() { _ = svc.Close() }()
	svc.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	agent := NewSubscriberAgent(SubscriberAgentConfig{
		Topic:         "events_ce",
		ConsumerGroup: "test-ce-group",
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}, handler)

	if err := agent.Start(ctx); err != nil {
		t.Fatalf("agent.Start failed: %v", err)
	}
	defer agent.Stop()

	// Wait for event 1
	select {
	case e1 := <-receivedEvents:
		if e1.ID() != "ce-struct-101" {
			t.Errorf("expected e1 ID ce-struct-101, got %s", e1.ID())
		}
		if e1.Type() != "io.service.order.created.v1" {
			t.Errorf("expected e1 Type io.service.order.created.v1, got %s", e1.Type())
		}
		if e1.Subject() != "orders/order-101" {
			t.Errorf("expected e1 Subject orders/order-101, got %s", e1.Subject())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event 1")
	}

	// Wait for event 2
	select {
	case e2 := <-receivedEvents:
		if e2.ID() != "ce-bin-202" {
			t.Errorf("expected e2 ID ce-bin-202, got %s", e2.ID())
		}
		if e2.Type() != "io.service.order.confirmed.v1" {
			t.Errorf("expected e2 Type io.service.order.confirmed.v1, got %s", e2.Type())
		}
		if e2.Subject() != "orders/order-102" {
			t.Errorf("expected e2 Subject orders/order-102, got %s", e2.Subject())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event 2")
	}
}
