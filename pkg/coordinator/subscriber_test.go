package coordinator

import (
	"context"
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

	// 1. Publish a CloudEvent
	ceStructured := cloudevents.NewEvent()
	ceStructured.SetID("ce-struct-101")
	ceStructured.SetSource("urn:service:publisher")
	ceStructured.SetType("io.service.order.created.v1")
	ceStructured.SetSubject("orders/order-101")
	_ = ceStructured.SetData(cloudevents.ApplicationJSON, map[string]string{
		"orderId": "order-101",
		"status":  "CREATED",
	})
	pbCE1, err := SDKToProtoCloudEvent(ceStructured)
	if err != nil {
		t.Fatalf("SDKToProtoCloudEvent 1: %v", err)
	}

	_, err = client.Publish(ctx, &pb.PublishRequest{
		Topic: "events_ce",
		Event: pbCE1,
	})
	if err != nil {
		t.Fatalf("publish structured CE: %v", err)
	}

	// 2. Publish a second CloudEvent
	ce2 := cloudevents.NewEvent()
	ce2.SetID("ce-bin-202")
	ce2.SetSource("urn:service:order-processor")
	ce2.SetType("io.service.order.confirmed.v1")
	ce2.SetSubject("orders/order-102")
	_ = ce2.SetData(cloudevents.ApplicationJSON, []byte(`{"orderId":"order-102","status":"CONFIRMED"}`))
	pbCE2, err := SDKToProtoCloudEvent(ce2)
	if err != nil {
		t.Fatalf("SDKToProtoCloudEvent 2: %v", err)
	}

	_, err = client.Publish(ctx, &pb.PublishRequest{
		Topic: "events_ce",
		Event: pbCE2,
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

func TestProtoToSDKCloudEvent_TimestampHandling(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)

	// 1. Proto with string timestamp
	pbStr := &pb.CloudEvent{
		Id:          "ts-str-1",
		Source:      "urn:test:source",
		SpecVersion: "1.0",
		Type:        "test.time",
		Attributes: map[string]*pb.CloudEventAttributeValue{
			"time": {
				Attr: &pb.CloudEventAttributeValue_CeString{
					CeString: now.Format(time.RFC3339Nano),
				},
			},
		},
	}

	eventStr, err := ProtoToSDKCloudEvent(pbStr)
	if err != nil {
		t.Fatalf("ProtoToSDKCloudEvent string time failed: %v", err)
	}
	if !eventStr.Time().Equal(now) {
		t.Errorf("expected parsed time %v, got %v", now, eventStr.Time())
	}
}
