package coordinator

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	pb "github.com/magnusp/watermill-coordinator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CloudEventHandler is invoked when a valid CloudEvent is received on the subscription stream.
// Return nil to acknowledge (ACK) the event, or an error to negative-acknowledge (NACK).
type CloudEventHandler func(ctx context.Context, event cloudevents.Event) error

// SubscriberAgentConfig holds configuration for running a subscriber agent.
type SubscriberAgentConfig struct {
	CoordinatorAddr string
	Topic           string
	ConsumerGroup   string
	DialOptions     []grpc.DialOption
}

// SubscriberAgent connects to the coordinator over gRPC and receives events,
// decoding them as CloudEvents and forwarding them to a CloudEventHandler.
type SubscriberAgent struct {
	cfg        SubscriberAgentConfig
	handler    CloudEventHandler
	grpcConn   *grpc.ClientConn
	client     pb.CoordinatorServiceClient
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// NewSubscriberAgent creates a new SubscriberAgent instance.
func NewSubscriberAgent(cfg SubscriberAgentConfig, handler CloudEventHandler) *SubscriberAgent {
	if cfg.CoordinatorAddr == "" {
		cfg.CoordinatorAddr = "localhost:50051"
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "default-subscriber"
	}
	return &SubscriberAgent{
		cfg:     cfg,
		handler: handler,
	}
}

// Start connects to the coordinator and starts consuming events asynchronously.
func (s *SubscriberAgent) Start(ctx context.Context) error {
	dialOpts := s.cfg.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}

	conn, err := grpc.NewClient(s.cfg.CoordinatorAddr, dialOpts...)
	if err != nil {
		return fmt.Errorf("dial coordinator at %s: %w", s.cfg.CoordinatorAddr, err)
	}
	s.grpcConn = conn
	s.client = pb.NewCoordinatorServiceClient(conn)

	runCtx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel

	s.wg.Add(1)
	go s.runLoop(runCtx)

	return nil
}

func (s *SubscriberAgent) runLoop(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := s.streamAndHandle(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[SubscriberAgent] Stream disconnected (%v), reconnecting in 1s...", err)
			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *SubscriberAgent) streamAndHandle(ctx context.Context) error {
	stream, err := s.client.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("open subscribe stream: %w", err)
	}

	// Send initial start subscription request
	err = stream.Send(&pb.SubscribeClientMessage{
		Action: &pb.SubscribeClientMessage_Start{
			Start: &pb.SubscribeRequest{
				Topic:         s.cfg.Topic,
				ConsumerGroup: s.cfg.ConsumerGroup,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("send start request: %w", err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("stream recv: %w", err)
		}

		event, err := ParseCloudEvent(msg.MessageId, msg.Payload, msg.Metadata)
		if err != nil {
			log.Printf("[SubscriberAgent] Failed to parse CloudEvent for message %s: %v (nacking)", msg.MessageId, err)
			_ = stream.Send(&pb.SubscribeClientMessage{
				Action: &pb.SubscribeClientMessage_Nack{
					Nack: &pb.NackRequest{
						MessageId: msg.MessageId,
						Reason:    err.Error(),
					},
				},
			})
			continue
		}

		// Dispatch to handler
		if err := s.handler(ctx, *event); err != nil {
			log.Printf("[SubscriberAgent] Handler returned error for event %s: %v (nacking)", event.ID(), err)
			_ = stream.Send(&pb.SubscribeClientMessage{
				Action: &pb.SubscribeClientMessage_Nack{
					Nack: &pb.NackRequest{
						MessageId: msg.MessageId,
						Reason:    err.Error(),
					},
				},
			})
		} else {
			_ = stream.Send(&pb.SubscribeClientMessage{
				Action: &pb.SubscribeClientMessage_Ack{
					Ack: &pb.AckRequest{
						MessageId: msg.MessageId,
					},
				},
			})
		}
	}
}

// Stop gracefully shuts down the subscriber agent and closes the gRPC connection.
func (s *SubscriberAgent) Stop() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	s.wg.Wait()
	if s.grpcConn != nil {
		_ = s.grpcConn.Close()
	}
}

// ParseCloudEvent decodes an incoming gRPC EventMessage (structured or binary) into a CloudEvent.
func ParseCloudEvent(messageID string, payload []byte, metadata map[string]string) (*cloudevents.Event, error) {
	// Mode 1: Check if payload is structured CloudEvents JSON
	contentType := metadata["content-type"]
	if contentType == "application/cloudevents+json" || contentType == "application/cloudevents+json; charset=utf-8" {
		event := cloudevents.NewEvent()
		if err := event.UnmarshalJSON(payload); err == nil && event.ID() != "" {
			return &event, nil
		}
	}

	// Try unmarshaling payload as structured CloudEvent directly even without explicit content-type
	event := cloudevents.NewEvent()
	if err := event.UnmarshalJSON(payload); err == nil && event.SpecVersion() != "" && event.ID() != "" && event.Type() != "" {
		return &event, nil
	}

	// Mode 2: Binary mode mapping from metadata headers
	// CloudEvents binary mode maps context attributes to headers/metadata (e.g. ce-type, ce-source, or type, source)
	event = cloudevents.NewEvent()
	id := getAttr(metadata, "id", messageID)
	if id != "" {
		event.SetID(id)
	} else {
		event.SetID(messageID)
	}

	source := getAttr(metadata, "source", "urn:event:source")
	event.SetSource(source)

	eventType := getAttr(metadata, "type", "event.unspecified")
	event.SetType(eventType)

	if subject := getAttr(metadata, "subject", ""); subject != "" {
		event.SetSubject(subject)
	}

	if tStr := getAttr(metadata, "time", ""); tStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, tStr); err == nil {
			event.SetTime(t)
		}
	}

	dataContentType := getAttr(metadata, "datacontenttype", "application/json")
	if err := event.SetData(dataContentType, payload); err != nil {
		return nil, fmt.Errorf("set event data: %w", err)
	}

	return &event, nil
}

func getAttr(m map[string]string, key, fallback string) string {
	if m == nil {
		return fallback
	}
	if val, ok := m["ce-"+key]; ok && val != "" {
		return val
	}
	if val, ok := m[key]; ok && val != "" {
		return val
	}
	return fallback
}
