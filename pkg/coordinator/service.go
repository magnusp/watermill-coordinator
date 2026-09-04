package coordinator

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sqlite/wmsqlitemodernc"
	"github.com/ThreeDotsLabs/watermill/message"
	pb "github.com/magnusp/watermill-coordinator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var validTopicRegex = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)

func validateTopic(topic string) error {
	if !validTopicRegex.MatchString(topic) {
		return fmt.Errorf("invalid topic name %q: must match regex ^[a-zA-Z0-9_.-]{1,64}$", topic)
	}
	return nil
}

// ServiceOptions holds configuration for embedding CoordinatorService.
type ServiceOptions struct {
	WriteTimeout time.Duration
	MaxInFlight  int
	Logger       watermill.LoggerAdapter
}

// Service implements the CoordinatorServiceServer gRPC interface.
type Service struct {
	pb.UnimplementedCoordinatorServiceServer

	db           *sql.DB
	publisher    message.Publisher
	writeTimeout time.Duration
	maxInFlight  int
	wmLogger     watermill.LoggerAdapter

	ensuredTopics sync.Map
	schemaMutex   sync.Mutex
}

// NewService creates a new embeddable CoordinatorService.
func NewService(db *sql.DB, opts ServiceOptions) (*Service, error) {
	if db == nil {
		return nil, fmt.Errorf("db must not be nil")
	}

	wmLogger := opts.Logger
	if wmLogger == nil {
		wmLogger = watermill.NewStdLogger(false, false)
	}

	writeTimeout := opts.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = 5 * time.Second
	}

	maxInFlight := opts.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = 50 // Safe default bounded window
	}

	pub, err := wmsqlitemodernc.NewPublisher(db, wmsqlitemodernc.PublisherOptions{
		InitializeSchema: true,
		Logger:           wmLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize watermill publisher: %w", err)
	}

	return &Service{
		db:           db,
		publisher:    pub,
		writeTimeout: writeTimeout,
		maxInFlight:  maxInFlight,
		wmLogger:     wmLogger,
	}, nil
}

// Register registers this coordinator service with an existing gRPC server.
func (s *Service) Register(server *grpc.Server) {
	pb.RegisterCoordinatorServiceServer(server, s)
}

// Close closes the underlying Watermill publisher.
func (s *Service) Close() error {
	if s.publisher != nil {
		return s.publisher.Close()
	}
	return nil
}

func (s *Service) ensureTopicSchema(topic string) error {
	if err := validateTopic(topic); err != nil {
		return err
	}

	if _, ok := s.ensuredTopics.Load(topic); ok {
		return nil
	}

	s.schemaMutex.Lock()
	defer s.schemaMutex.Unlock()

	if _, ok := s.ensuredTopics.Load(topic); ok {
		return nil
	}

	tableName := fmt.Sprintf("watermill_%s", topic)
	indexName := fmt.Sprintf("idx_watermill_%s_uuid", topic)

	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS '%s' (
			'offset' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			uuid TEXT NOT NULL,
			created_at TEXT NOT NULL,
			payload BLOB,
			metadata JSON NOT NULL
		);
	`, tableName)

	createIndexQuery := fmt.Sprintf(`
		CREATE UNIQUE INDEX IF NOT EXISTS '%s' ON '%s' (uuid);
	`, indexName, tableName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := s.db.ExecContext(ctx, createTableQuery); err != nil {
		return fmt.Errorf("create table %s: %w", tableName, err)
	}

	if _, err := s.db.ExecContext(ctx, createIndexQuery); err != nil {
		return fmt.Errorf("create unique index %s: %w", indexName, err)
	}

	s.ensuredTopics.Store(topic, true)
	return nil
}

// Publish publishes an event idempotently based on event ID.
func (s *Service) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	topic := strings.TrimSpace(req.Topic)
	if topic == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic must be specified")
	}

	if err := validateTopic(topic); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	if req.Event == nil {
		return nil, status.Errorf(codes.InvalidArgument, "event cannot be nil")
	}

	msgID := strings.TrimSpace(req.Event.Id)
	if msgID == "" {
		msgID = watermill.NewUUID()
		req.Event.Id = msgID
	}

	if err := s.ensureTopicSchema(topic); err != nil {
		log.Printf("[GRPC] Failed to ensure schema for topic %s: %v", topic, err)
		return nil, status.Errorf(codes.Internal, "failed to initialize topic schema: %v", err)
	}

	msg, err := CloudEventToWatermillMessage(req.Event)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid cloudevent: %v", err)
	}

	now := time.Now().UTC()
	timestampStr := now.Format(time.RFC3339Nano)
	msg.Metadata.Set("ingress_published_at", timestampStr)
	msg.Metadata.Set("ingress_topic", topic)

	err = s.publisher.Publish(topic, msg)
	if err != nil {
		errLower := strings.ToLower(err.Error())
		if strings.Contains(errLower, "unique") || strings.Contains(errLower, "constraint failed") {
			log.Printf("[GRPC] Duplicate publish detected for message_id=%s on topic=%s (idempotent no-op)", msgID, topic)
			return &pb.PublishResponse{
				Status:    pb.PublishResponse_ALREADY_EXISTS,
				MessageId: msgID,
				Topic:     topic,
				Timestamp: timestampStr,
			}, nil
		}

		log.Printf("[GRPC] Failed to publish message %s to topic %s: %v", msgID, topic, err)
		return nil, status.Errorf(codes.Internal, "failed to publish message: %v", err)
	}

	log.Printf("[GRPC] Successfully published event %s to topic %s (bytes=%d)", msgID, topic, len(msg.Payload))

	return &pb.PublishResponse{
		Status:    pb.PublishResponse_PUBLISHED,
		MessageId: msgID,
		Topic:     topic,
		Timestamp: timestampStr,
	}, nil
}

// Subscribe provides bi-directional streaming of events and client-side acknowledgments.
func (s *Service) Subscribe(stream pb.CoordinatorService_SubscribeServer) error {
	ctx := stream.Context()

	initMsg, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return status.Errorf(codes.InvalidArgument, "failed to read initial subscribe request: %v", err)
	}

	startReq := initMsg.GetStart()
	if startReq == nil {
		return status.Errorf(codes.InvalidArgument, "first client message must be a start SubscribeRequest")
	}

	topic := strings.TrimSpace(startReq.Topic)
	if topic == "" {
		return status.Errorf(codes.InvalidArgument, "topic must be specified")
	}

	if err := validateTopic(topic); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}

	consumerGroup := strings.TrimSpace(startReq.ConsumerGroup)
	if consumerGroup == "" {
		consumerGroup = "default_group"
	}

	if err := s.ensureTopicSchema(topic); err != nil {
		return status.Errorf(codes.Internal, "failed to initialize topic schema: %v", err)
	}

	subscriber, err := wmsqlitemodernc.NewSubscriber(s.db, wmsqlitemodernc.SubscriberOptions{
		ConsumerGroupMatcher: wmsqlitemodernc.NewStaticConsumerGroupMatcher(consumerGroup),
		InitializeSchema:     true,
		BatchSize:            1,
		PollInterval:         25 * time.Millisecond,
		LockTimeout:          5 * time.Second,
		Logger:               s.wmLogger,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create subscriber: %v", err)
	}
	defer func() { _ = subscriber.Close() }()

	messagesCh, err := subscriber.Subscribe(ctx, topic)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to subscribe to topic: %v", err)
	}

	log.Printf("[GRPC] Client subscribed (bi-di) to topic=%s, consumer_group=%s", topic, consumerGroup)

	ackCh := make(chan *pb.SubscribeClientMessage, 100)
	recvErrCh := make(chan error, 1)

	go func() {
		for {
			clientMsg, err := stream.Recv()
			if err != nil {
				recvErrCh <- err
				return
			}
			select {
			case ackCh <- clientMsg:
			case <-ctx.Done():
				return
			}
		}
	}()

	pendingAcks := make(map[string]*message.Message)

	for {
		// Bounded Flow Control / Prefetch Window:
		// When unacknowledged in-flight messages reach maxInFlight, disable consuming
		// from messagesCh until client acks arrive to prevent heap exhaustion.
		var activeMessagesCh <-chan *message.Message
		if len(pendingAcks) < s.maxInFlight {
			activeMessagesCh = messagesCh
		}

		select {
		case <-ctx.Done():
			log.Printf("[GRPC] Subscription stream context done for topic=%s, group=%s", topic, consumerGroup)
			for _, m := range pendingAcks {
				m.Nack()
			}
			return ctx.Err()

		case err := <-recvErrCh:
			if err != io.EOF {
				log.Printf("[GRPC] Subscription stream recv error: %v", err)
			}
			for _, m := range pendingAcks {
				m.Nack()
			}
			if err == io.EOF {
				return nil
			}
			return err

		case clientMsg := <-ackCh:
			switch act := clientMsg.Action.(type) {
			case *pb.SubscribeClientMessage_Ack:
				msgID := act.Ack.MessageId
				if unackedMsg, exists := pendingAcks[msgID]; exists {
					unackedMsg.Ack()
					delete(pendingAcks, msgID)
					log.Printf("[GRPC] Client acknowledged message %s (offset advanced, in-flight=%d/%d)", msgID, len(pendingAcks), s.maxInFlight)
				}
			case *pb.SubscribeClientMessage_Nack:
				msgID := act.Nack.MessageId
				if unackedMsg, exists := pendingAcks[msgID]; exists {
					unackedMsg.Nack()
					delete(pendingAcks, msgID)
					log.Printf("[GRPC] Client nacked message %s (reason=%q), returned to queue (in-flight=%d/%d)", msgID, act.Nack.Reason, len(pendingAcks), s.maxInFlight)
				}
			}

		case msg, ok := <-activeMessagesCh:
			if !ok {
				log.Printf("[GRPC] Messages channel closed for topic=%s", topic)
				return nil
			}

			pbEvent, err := WatermillMessageToCloudEvent(msg)
			if err != nil {
				msg.Nack()
				log.Printf("[GRPC] Error converting message %s to CloudEvent: %v", msg.UUID, err)
				return err
			}

			eventMsg := &pb.EventMessage{
				Topic: topic,
				Event: pbEvent,
			}

			if err := stream.Send(eventMsg); err != nil {
				msg.Nack()
				log.Printf("[GRPC] Error streaming event %s: %v", msg.UUID, err)
				return err
			}

			pendingAcks[msg.UUID] = msg
		}
	}
}

// CheckHealth provides health status.
func (s *Service) CheckHealth(ctx context.Context, _ *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := s.db.PingContext(pingCtx); err != nil {
		return &pb.HealthCheckResponse{
			Status: pb.HealthCheckResponse_NOT_SERVING,
		}, status.Errorf(codes.Unavailable, "database unavailable: %v", err)
	}

	return &pb.HealthCheckResponse{
		Status: pb.HealthCheckResponse_SERVING,
	}, nil
}
