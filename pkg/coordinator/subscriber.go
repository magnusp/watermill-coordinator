package coordinator

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	pb "github.com/magnusp/watermill-coordinator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	MaxRetries      int                                                            // Max delivery attempts before treating as poison pill (0 = unlimited)
	RetryBackoff    time.Duration                                                  // Pause between retry attempts to prevent tight CPU looping
	OnPoisonEvent   func(ctx context.Context, msg *pb.EventMessage, reason error) // Hook called when max retries exceeded
}

// SubscriberAgent connects to the coordinator over gRPC and receives events,
// decoding them as CloudEvents and forwarding them to a CloudEventHandler.
type SubscriberAgent struct {
	cfg         SubscriberAgentConfig
	handler     CloudEventHandler
	grpcConn    *grpc.ClientConn
	client      pb.CoordinatorServiceClient
	cancelFunc  context.CancelFunc
	wg          sync.WaitGroup
	retryCounts map[string]int
}

// NewSubscriberAgent creates a new SubscriberAgent instance.
func NewSubscriberAgent(cfg SubscriberAgentConfig, handler CloudEventHandler) *SubscriberAgent {
	if cfg.CoordinatorAddr == "" {
		cfg.CoordinatorAddr = "localhost:50051"
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "default-subscriber"
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}
	return &SubscriberAgent{
		cfg:         cfg,
		handler:     handler,
		retryCounts: make(map[string]int),
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

		if msg.Event == nil {
			continue
		}

		handleFailure := func(msgID string, failErr error) {
			s.retryCounts[msgID]++
			if s.cfg.MaxRetries > 0 && s.retryCounts[msgID] > s.cfg.MaxRetries {
				log.Printf("[SubscriberAgent] Message %s exceeded max retries (%d), marking as poison: %v", msgID, s.cfg.MaxRetries, failErr)
				if s.cfg.OnPoisonEvent != nil {
					s.cfg.OnPoisonEvent(ctx, msg, failErr)
				}
				// ACK to advance offset and prevent permanent queue blocking
				_ = stream.Send(&pb.SubscribeClientMessage{
					Action: &pb.SubscribeClientMessage_Ack{
						Ack: &pb.AckRequest{
							MessageId: msgID,
						},
					},
				})
				delete(s.retryCounts, msgID)
				return
			}

			// Backoff before sending NACK to avoid tight CPU loop
			if s.cfg.RetryBackoff > 0 {
				select {
				case <-time.After(s.cfg.RetryBackoff):
				case <-ctx.Done():
					return
				}
			}

			_ = stream.Send(&pb.SubscribeClientMessage{
				Action: &pb.SubscribeClientMessage_Nack{
					Nack: &pb.NackRequest{
						MessageId: msgID,
						Reason:    failErr.Error(),
					},
				},
			})
		}

		event, err := ProtoToSDKCloudEvent(msg.Event)
		if err != nil {
			log.Printf("[SubscriberAgent] Failed to parse CloudEvent for message %s: %v (handling failure)", msg.Event.Id, err)
			handleFailure(msg.Event.Id, err)
			continue
		}

		// Dispatch to handler
		if err := s.handler(ctx, *event); err != nil {
			log.Printf("[SubscriberAgent] Handler returned error for event %s: %v (handling failure)", event.ID(), err)
			handleFailure(event.ID(), err)
		} else {
			delete(s.retryCounts, event.ID())
			_ = stream.Send(&pb.SubscribeClientMessage{
				Action: &pb.SubscribeClientMessage_Ack{
					Ack: &pb.AckRequest{
						MessageId: event.ID(),
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

// ProtoToSDKCloudEvent converts a Protobuf CloudEvent message to a cloudevents.Event.
func ProtoToSDKCloudEvent(pbEvent *pb.CloudEvent) (*cloudevents.Event, error) {
	if pbEvent == nil {
		return nil, fmt.Errorf("proto event cannot be nil")
	}

	event := cloudevents.NewEvent()
	event.SetID(pbEvent.Id)
	event.SetSource(pbEvent.Source)
	event.SetType(pbEvent.Type)
	if pbEvent.SpecVersion != "" {
		event.SetSpecVersion(pbEvent.SpecVersion)
	}

	// Map attributes
	for k, v := range pbEvent.Attributes {
		if v == nil {
			continue
		}
		switch attr := v.Attr.(type) {
		case *pb.CloudEventAttributeValue_CeString:
			if k == "subject" {
				event.SetSubject(attr.CeString)
			} else if k == "datacontenttype" {
				event.SetDataContentType(attr.CeString)
			} else if k == "time" {
				if t, err := time.Parse(time.RFC3339Nano, attr.CeString); err == nil {
					event.SetTime(t)
				} else {
					event.SetExtension(k, attr.CeString)
				}
			} else {
				event.SetExtension(k, attr.CeString)
			}
		case *pb.CloudEventAttributeValue_CeBoolean:
			event.SetExtension(k, attr.CeBoolean)
		case *pb.CloudEventAttributeValue_CeInteger:
			event.SetExtension(k, attr.CeInteger)
		case *pb.CloudEventAttributeValue_CeBytes:
			event.SetExtension(k, attr.CeBytes)
		case *pb.CloudEventAttributeValue_CeUri:
			event.SetExtension(k, attr.CeUri)
		case *pb.CloudEventAttributeValue_CeUriRef:
			event.SetExtension(k, attr.CeUriRef)
		case *pb.CloudEventAttributeValue_CeTimestamp:
			if attr.CeTimestamp != nil {
				t := attr.CeTimestamp.AsTime()
				if k == "time" {
					event.SetTime(t)
				} else {
					event.SetExtension(k, t)
				}
			}
		}
	}

	// Map data
	switch d := pbEvent.Data.(type) {
	case *pb.CloudEvent_BinaryData:
		contentType := event.DataContentType()
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		_ = event.SetData(contentType, d.BinaryData)
	case *pb.CloudEvent_TextData:
		contentType := event.DataContentType()
		if contentType == "" {
			contentType = "text/plain"
		}
		_ = event.SetData(contentType, d.TextData)
	}

	return &event, nil
}

// SDKToProtoCloudEvent converts a cloudevents.Event to a Protobuf CloudEvent message.
func SDKToProtoCloudEvent(event cloudevents.Event) (*pb.CloudEvent, error) {
	pbEvent := &pb.CloudEvent{
		Id:          event.ID(),
		Source:      event.Source(),
		SpecVersion: event.SpecVersion(),
		Type:        event.Type(),
		Attributes:  make(map[string]*pb.CloudEventAttributeValue),
	}

	if event.Subject() != "" {
		pbEvent.Attributes["subject"] = &pb.CloudEventAttributeValue{
			Attr: &pb.CloudEventAttributeValue_CeString{CeString: event.Subject()},
		}
	}

	if !event.Time().IsZero() {
		pbEvent.Attributes["time"] = &pb.CloudEventAttributeValue{
			Attr: &pb.CloudEventAttributeValue_CeTimestamp{
				CeTimestamp: timestamppb.New(event.Time()),
			},
		}
	}

	if event.DataContentType() != "" {
		pbEvent.Attributes["datacontenttype"] = &pb.CloudEventAttributeValue{
			Attr: &pb.CloudEventAttributeValue_CeString{CeString: event.DataContentType()},
		}
	}

	for k, v := range event.Extensions() {
		switch val := v.(type) {
		case string:
			pbEvent.Attributes[k] = &pb.CloudEventAttributeValue{
				Attr: &pb.CloudEventAttributeValue_CeString{CeString: val},
			}
		case bool:
			pbEvent.Attributes[k] = &pb.CloudEventAttributeValue{
				Attr: &pb.CloudEventAttributeValue_CeBoolean{CeBoolean: val},
			}
		case int32:
			pbEvent.Attributes[k] = &pb.CloudEventAttributeValue{
				Attr: &pb.CloudEventAttributeValue_CeInteger{CeInteger: val},
			}
		case int:
			pbEvent.Attributes[k] = &pb.CloudEventAttributeValue{
				Attr: &pb.CloudEventAttributeValue_CeInteger{CeInteger: int32(val)},
			}
		case []byte:
			pbEvent.Attributes[k] = &pb.CloudEventAttributeValue{
				Attr: &pb.CloudEventAttributeValue_CeBytes{CeBytes: val},
			}
		case time.Time:
			pbEvent.Attributes[k] = &pb.CloudEventAttributeValue{
				Attr: &pb.CloudEventAttributeValue_CeTimestamp{
					CeTimestamp: timestamppb.New(val),
				},
			}
		default:
			pbEvent.Attributes[k] = &pb.CloudEventAttributeValue{
				Attr: &pb.CloudEventAttributeValue_CeString{CeString: fmt.Sprintf("%v", val)},
			}
		}
	}

	if len(event.Data()) > 0 {
		pbEvent.Data = &pb.CloudEvent_BinaryData{BinaryData: event.Data()}
	}

	return pbEvent, nil
}

// CloudEventToWatermillMessage maps a CloudEvent proto into a Watermill Message.
func CloudEventToWatermillMessage(pbEvent *pb.CloudEvent) (*message.Message, error) {
	if pbEvent == nil {
		return nil, fmt.Errorf("cloudevent proto cannot be nil")
	}

	msgID := pbEvent.Id
	if msgID == "" {
		msgID = watermill.NewUUID()
	}

	var payload []byte
	switch d := pbEvent.Data.(type) {
	case *pb.CloudEvent_BinaryData:
		payload = d.BinaryData
	case *pb.CloudEvent_TextData:
		payload = []byte(d.TextData)
	}

	msg := message.NewMessage(msgID, payload)
	msg.Metadata.Set("ce-id", pbEvent.Id)
	msg.Metadata.Set("ce-source", pbEvent.Source)
	msg.Metadata.Set("ce-type", pbEvent.Type)
	msg.Metadata.Set("ce-specversion", pbEvent.SpecVersion)

	for k, v := range pbEvent.Attributes {
		if v == nil {
			continue
		}
		switch attr := v.Attr.(type) {
		case *pb.CloudEventAttributeValue_CeString:
			msg.Metadata.Set("ce-"+k, attr.CeString)
			if k == "datacontenttype" {
				msg.Metadata.Set("content-type", attr.CeString)
			}
		case *pb.CloudEventAttributeValue_CeBoolean:
			msg.Metadata.Set("ce-"+k, fmt.Sprintf("%t", attr.CeBoolean))
		case *pb.CloudEventAttributeValue_CeInteger:
			msg.Metadata.Set("ce-"+k, fmt.Sprintf("%d", attr.CeInteger))
		case *pb.CloudEventAttributeValue_CeTimestamp:
			if attr.CeTimestamp != nil {
				msg.Metadata.Set("ce-"+k, attr.CeTimestamp.AsTime().Format(time.RFC3339Nano))
			}
		}
	}

	return msg, nil
}

// WatermillMessageToCloudEvent maps a Watermill Message into a CloudEvent proto.
func WatermillMessageToCloudEvent(msg *message.Message) (*pb.CloudEvent, error) {
	if msg == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}

	id := getAttr(msg.Metadata, "id", msg.UUID)
	source := getAttr(msg.Metadata, "source", "urn:event:source")
	eventType := getAttr(msg.Metadata, "type", "event.unspecified")
	specVersion := getAttr(msg.Metadata, "specversion", "1.0")

	pbEvent := &pb.CloudEvent{
		Id:          id,
		Source:      source,
		SpecVersion: specVersion,
		Type:        eventType,
		Attributes:  make(map[string]*pb.CloudEventAttributeValue),
	}

	for k, v := range msg.Metadata {
		if strings.HasPrefix(k, "ce-") {
			attrName := strings.TrimPrefix(k, "ce-")
			switch attrName {
			case "id", "source", "type", "specversion":
				// Already mapped
			case "time":
				if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
					pbEvent.Attributes["time"] = &pb.CloudEventAttributeValue{
						Attr: &pb.CloudEventAttributeValue_CeTimestamp{CeTimestamp: timestamppb.New(t)},
					}
				} else {
					pbEvent.Attributes["time"] = &pb.CloudEventAttributeValue{
						Attr: &pb.CloudEventAttributeValue_CeString{CeString: v},
					}
				}
			default:
				pbEvent.Attributes[attrName] = &pb.CloudEventAttributeValue{
					Attr: &pb.CloudEventAttributeValue_CeString{CeString: v},
				}
			}
		}
	}

	if len(msg.Payload) > 0 {
		pbEvent.Data = &pb.CloudEvent_BinaryData{BinaryData: msg.Payload}
	}

	return pbEvent, nil
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
