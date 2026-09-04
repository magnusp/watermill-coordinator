package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	pb "github.com/magnusp/watermill-coordinator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	_ "modernc.org/sqlite"
)

const bufSize = 1024 * 1024

func setupTestGRPC(t *testing.T) (*sql.DB, pb.CoordinatorServiceClient, func()) {
	t.Helper()

	db, err := sql.Open("sqlite", "file:memtest_pkg?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}

	wmLogger := watermill.NewStdLogger(false, false)
	svc, err := NewService(db, ServiceOptions{
		WriteTimeout: 2 * time.Second,
		Logger:       wmLogger,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	svc.Register(srv)

	go func() {
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("test server err: %v", err)
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	client := pb.NewCoordinatorServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = svc.Close()
		_ = db.Close()
	}

	return db, client, cleanup
}

func TestGRPC_CheckHealth(t *testing.T) {
	_, client, cleanup := setupTestGRPC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.CheckHealth(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}

	if resp.Status != pb.HealthCheckResponse_SERVING {
		t.Errorf("expected status SERVING, got %v", resp.Status)
	}
}

func TestGRPC_Publish_Success_And_Dedup(t *testing.T) {
	db, client, cleanup := setupTestGRPC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := &pb.PublishRequest{
		Topic: "orders_topic",
		Event: &pb.CloudEvent{
			Id:          "order-msg-101",
			Source:      "urn:spring-modulith",
			Type:        "order.created",
			SpecVersion: "1.0",
			Data:        &pb.CloudEvent_BinaryData{BinaryData: []byte(`{"order_id": 101, "total": 49.99}`)},
		},
	}

	resp, err := client.Publish(ctx, req)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if resp.Status != pb.PublishResponse_PUBLISHED {
		t.Errorf("expected PUBLISHED status, got %v", resp.Status)
	}
	if resp.MessageId != "order-msg-101" {
		t.Errorf("expected message_id order-msg-101, got %s", resp.MessageId)
	}
	if resp.Topic != "orders_topic" {
		t.Errorf("expected topic orders_topic, got %s", resp.Topic)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM watermill_orders_topic WHERE uuid = 'order-msg-101'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query db: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}

	dupResp, err := client.Publish(ctx, req)
	if err != nil {
		t.Fatalf("duplicate publish failed unexpectedly: %v", err)
	}

	if dupResp.Status != pb.PublishResponse_ALREADY_EXISTS {
		t.Errorf("expected ALREADY_EXISTS status on duplicate, got %v", dupResp.Status)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM watermill_orders_topic WHERE uuid = 'order-msg-101'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query db: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1 after duplicate, got %d", count)
	}
}

func TestGRPC_Publish_ValidationErrors(t *testing.T) {
	_, client, cleanup := setupTestGRPC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &pb.PublishRequest{
		Topic: "test_topic",
		Event: nil,
	}

	_, err := client.Publish(ctx, req)
	if err == nil {
		t.Fatal("expected error on nil event, got nil")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected codes.InvalidArgument, got %v", err)
	}
}

func TestGRPC_Publish_TopicValidation(t *testing.T) {
	_, client, cleanup := setupTestGRPC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	badTopics := []string{
		"",
		" ",
		"topic with spaces",
		"topic;drop table",
		"topic'--",
		"topic!@#$",
		"a-very-long-topic-name-that-exceeds-sixty-four-characters-limit-which-is-invalid",
	}

	for _, bad := range badTopics {
		req := &pb.PublishRequest{
			Topic: bad,
			Event: &pb.CloudEvent{
				Id:          "msg-test",
				Source:      "urn:test",
				Type:        "test.event",
				SpecVersion: "1.0",
				Data:        &pb.CloudEvent_BinaryData{BinaryData: []byte("test")},
			},
		}
		_, err := client.Publish(ctx, req)
		if err == nil {
			t.Fatalf("expected error for invalid topic %q, got nil", bad)
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument for %q, got %v", bad, err)
		}
	}
}

func TestGRPC_Subscribe_BiDirectional(t *testing.T) {
	_, client, cleanup := setupTestGRPC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pubReq1 := &pb.PublishRequest{
		Topic: "stream_topic",
		Event: &pb.CloudEvent{
			Id:          "stream-msg-1",
			Source:      "urn:test",
			Type:        "stream.event",
			SpecVersion: "1.0",
			Data:        &pb.CloudEvent_BinaryData{BinaryData: []byte(`first`)},
		},
	}
	if _, err := client.Publish(ctx, pubReq1); err != nil {
		t.Fatalf("publish 1 failed: %v", err)
	}

	pubReq2 := &pb.PublishRequest{
		Topic: "stream_topic",
		Event: &pb.CloudEvent{
			Id:          "stream-msg-2",
			Source:      "urn:test",
			Type:        "stream.event",
			SpecVersion: "1.0",
			Data:        &pb.CloudEvent_BinaryData{BinaryData: []byte(`second`)},
		},
	}
	if _, err := client.Publish(ctx, pubReq2); err != nil {
		t.Fatalf("publish 2 failed: %v", err)
	}

	stream, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe stream failed: %v", err)
	}

	if err := stream.Send(&pb.SubscribeClientMessage{
		Action: &pb.SubscribeClientMessage_Start{
			Start: &pb.SubscribeRequest{
				Topic:         "stream_topic",
				ConsumerGroup: "java_service_group",
			},
		},
	}); err != nil {
		t.Fatalf("send start error: %v", err)
	}

	receivedIDs := make([]string, 0, 2)
	for len(receivedIDs) < 2 {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream recv error: %v", err)
		}
		if msg.Event == nil {
			continue
		}
		receivedIDs = append(receivedIDs, msg.Event.Id)

		if err := stream.Send(&pb.SubscribeClientMessage{
			Action: &pb.SubscribeClientMessage_Ack{
				Ack: &pb.AckRequest{
					MessageId: msg.Event.Id,
				},
			},
		}); err != nil {
			t.Fatalf("send ack error: %v", err)
		}
	}

	if len(receivedIDs) != 2 {
		t.Fatalf("expected 2 received events, got %d", len(receivedIDs))
	}
	if receivedIDs[0] != "stream-msg-1" || receivedIDs[1] != "stream-msg-2" {
		t.Errorf("unexpected message sequence: %v", receivedIDs)
	}
}

func TestResolveDriverAndDSN(t *testing.T) {
	tests := []struct {
		input          string
		expectedDriver string
	}{
		{"http://127.0.0.1:8080", "libsql"},
		{"https://example.com/db", "libsql"},
		{"libsql://mydb.turso.io", "libsql"},
		{"ws://127.0.0.1:8080", "libsql"},
		{"wss://mydb.turso.io", "libsql"},
		{"dev.db", "sqlite"},
		{"/path/to/my.db", "sqlite"},
		{"file:memdb1?mode=memory&cache=shared", "sqlite"},
	}

	for _, tc := range tests {
		driver, _ := ResolveDriverAndDSN(tc.input)
		if driver != tc.expectedDriver {
			t.Errorf("for input %q: expected driver %q, got %q", tc.input, tc.expectedDriver, driver)
		}
	}
}

func TestSafePragmas(t *testing.T) {
	db, err := InitDB("file:test_pragmas?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	var foreignKeys int
	if err := db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatalf("query foreign_keys: %v", err)
	}
	if foreignKeys != 1 {
		t.Errorf("expected foreign_keys=1, got %d", foreignKeys)
	}

	var busyTimeout int
	if err := db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatalf("query busy_timeout: %v", err)
	}
	if busyTimeout < 5000 {
		t.Errorf("expected busy_timeout >= 5000, got %d", busyTimeout)
	}

	var tempStore int
	if err := db.QueryRow("PRAGMA temp_store").Scan(&tempStore); err != nil {
		t.Fatalf("query temp_store: %v", err)
	}
	if tempStore != 2 {
		t.Errorf("expected temp_store=2 (MEMORY), got %d", tempStore)
	}
}

func TestAssertSqldPrimaryNode(t *testing.T) {
	// 1. Primary server mockup
	primaryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/nodes" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"role": "primary", "index": 42}`))
	}))
	defer primaryServer.Close()

	if err := AssertSqldPrimaryNode(primaryServer.URL, ""); err != nil {
		t.Fatalf("expected primary to pass assertion, got: %v", err)
	}

	// 2. Replica server mockup
	replicaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/nodes" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"role": "replica", "primary_url": "http://sqld-primary:5001"}`))
	}))
	defer replicaServer.Close()

	err := AssertSqldPrimaryNode(replicaServer.URL, "")
	if err == nil {
		t.Fatal("expected replica to fail assertion, got nil")
	}
	if !strings.Contains(err.Error(), "coordinator strictly requires a primary writer") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestTursoCloudPrimaryResolution(t *testing.T) {
	// 1. Mock Turso Platform API
	mockTursoAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/v1/organizations/myorg/databases/orders-db/instances" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"instances": [
				{"name": "ams", "type": "replica", "hostname": "orders-db-ams-myorg.turso.io", "region": "ams"},
				{"name": "fra", "type": "primary", "hostname": "orders-db-fra-myorg.turso.io", "region": "fra"}
			]
		}`))
	}))
	defer mockTursoAPI.Close()

	// 2. Test hostname parser
	dbName, orgSlug, err := parseTursoHostname("orders-db-myorg.turso.io")
	if err != nil {
		t.Fatalf("parseTursoHostname failed: %v", err)
	}
	if dbName != "orders-db" || orgSlug != "myorg" {
		t.Errorf("expected orders-db / myorg, got %s / %s", dbName, orgSlug)
	}

	// 3. Test isTursoCloudHost
	if !isTursoCloudHost("libsql://orders-db-myorg.turso.io?authToken=xyz") {
		t.Errorf("expected isTursoCloudHost to be true")
	}
	if isTursoCloudHost("http://127.0.0.1:8080") {
		t.Errorf("expected isTursoCloudHost to be false for local sqld")
	}
	if isTursoCloudHost("file:dev.db") {
		t.Errorf("expected isTursoCloudHost to be false for local sqlite")
	}
}

func TestJepsen_DisconnectWithoutAck(t *testing.T) {
	_, client, cleanup := setupTestGRPC(t)
	defer cleanup()

	ctx := context.Background()

	pubReq := &pb.PublishRequest{
		Topic: "resilient_topic",
		Event: &pb.CloudEvent{
			Id:          "msg-crash-test",
			Source:      "urn:test",
			Type:        "crash.event",
			SpecVersion: "1.0",
			Data:        &pb.CloudEvent_BinaryData{BinaryData: []byte(`important-payload`)},
		},
	}
	if _, err := client.Publish(ctx, pubReq); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	stream1Ctx, stream1Cancel := context.WithCancel(ctx)
	stream1, err := client.Subscribe(stream1Ctx)
	if err != nil {
		t.Fatalf("subscribe stream1 failed: %v", err)
	}

	_ = stream1.Send(&pb.SubscribeClientMessage{
		Action: &pb.SubscribeClientMessage_Start{
			Start: &pb.SubscribeRequest{
				Topic:         "resilient_topic",
				ConsumerGroup: "resilient_group",
			},
		},
	})

	msg, err := stream1.Recv()
	if err != nil {
		t.Fatalf("stream1 recv error: %v", err)
	}
	if msg.Event == nil || msg.Event.Id != "msg-crash-test" {
		t.Fatalf("expected msg-crash-test, got %v", msg.Event)
	}

	stream1Cancel()

	time.Sleep(1200 * time.Millisecond)

	stream2Ctx, stream2Cancel := context.WithTimeout(ctx, 5*time.Second)
	defer stream2Cancel()

	stream2, err := client.Subscribe(stream2Ctx)
	if err != nil {
		t.Fatalf("subscribe stream2 failed: %v", err)
	}

	_ = stream2.Send(&pb.SubscribeClientMessage{
		Action: &pb.SubscribeClientMessage_Start{
			Start: &pb.SubscribeRequest{
				Topic:         "resilient_topic",
				ConsumerGroup: "resilient_group",
			},
		},
	})

	redeliveredMsg, err := stream2.Recv()
	if err != nil {
		t.Fatalf("stream2 recv error (message was lost after crash!): %v", err)
	}
	if redeliveredMsg.Event == nil || redeliveredMsg.Event.Id != "msg-crash-test" {
		t.Fatalf("expected redelivered message msg-crash-test, got %v", redeliveredMsg.Event)
	}

	_ = stream2.Send(&pb.SubscribeClientMessage{
		Action: &pb.SubscribeClientMessage_Ack{
			Ack: &pb.AckRequest{
				MessageId: redeliveredMsg.Event.Id,
			},
		},
	})
}

// TestBoundedFlowControl verifies that the coordinator pauses yielding from the database
// when in-flight unacknowledged messages reach MaxInFlight window limit.
func TestBoundedFlowControl(t *testing.T) {
	db, err := sql.Open("sqlite", "file:memtest_flow?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer func() { _ = db.Close() }()

	wmLogger := watermill.NewStdLogger(false, false)
	// Window limit set strictly to 2
	svc, err := NewService(db, ServiceOptions{
		WriteTimeout: 2 * time.Second,
		MaxInFlight:  2,
		Logger:       wmLogger,
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	defer func() { _ = svc.Close() }()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	svc.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := pb.NewCoordinatorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Publish 4 messages to the topic
	for i := 1; i <= 4; i++ {
		msgID := "flow-msg-" + string(rune('0'+i))
		_, err := client.Publish(ctx, &pb.PublishRequest{
			Topic: "flow_topic",
			Event: &pb.CloudEvent{
				Id:          msgID,
				Source:      "urn:test",
				Type:        "flow.event",
				SpecVersion: "1.0",
				Data:        &pb.CloudEvent_BinaryData{BinaryData: []byte("test")},
			},
		})
		if err != nil {
			t.Fatalf("publish %d err: %v", i, err)
		}
	}

	// 2. Open subscription
	stream, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe err: %v", err)
	}

	_ = stream.Send(&pb.SubscribeClientMessage{
		Action: &pb.SubscribeClientMessage_Start{
			Start: &pb.SubscribeRequest{
				Topic:         "flow_topic",
				ConsumerGroup: "flow_group",
			},
		},
	})

	// 3. Receive message 1
	m1, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv 1: %v", err)
	}
	if m1.Event == nil || m1.Event.Id != "flow-msg-1" {
		t.Errorf("expected flow-msg-1, got %v", m1.Event)
	}

	// 4. In Watermill SQLite, unacked message prevents next offset until acked (inherent strict order)
	// Try to read 2nd message before acking 1st -> should block
	read2Ch := make(chan *pb.EventMessage, 1)
	go func() {
		m2, _ := stream.Recv()
		if m2 != nil {
			read2Ch <- m2
		}
	}()

	select {
	case m2 := <-read2Ch:
		t.Fatalf("unexpectedly received 2nd message %v before acking 1st", m2.Event)
	case <-time.After(200 * time.Millisecond):
		// Expected: blocked until ack
	}

	// 5. Ack message 1 -> frees subscriber to yield next message
	_ = stream.Send(&pb.SubscribeClientMessage{
		Action: &pb.SubscribeClientMessage_Ack{
			Ack: &pb.AckRequest{
				MessageId: m1.Event.Id,
			},
		},
	})

	// 6. Now 2nd message unblocks and arrives
	select {
	case m2 := <-read2Ch:
		if m2.Event == nil || m2.Event.Id != "flow-msg-2" {
			t.Errorf("expected flow-msg-2, got %v", m2.Event)
		}
		// Ack message 2
		_ = stream.Send(&pb.SubscribeClientMessage{
			Action: &pb.SubscribeClientMessage_Ack{
				Ack: &pb.AckRequest{MessageId: m2.Event.Id},
			},
		})
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for 2nd message after acking 1st")
	}

	// 7. Receive message 3
	m3, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv 3: %v", err)
	}
	if m3.Event == nil || m3.Event.Id != "flow-msg-3" {
		t.Errorf("expected flow-msg-3, got %v", m3.Event)
	}
	_ = stream.Send(&pb.SubscribeClientMessage{
		Action: &pb.SubscribeClientMessage_Ack{
			Ack: &pb.AckRequest{MessageId: m3.Event.Id},
		},
	})
}


// FuzzParseTursoHostname tests that parseTursoHostname never panics on arbitrary string input.
func FuzzParseTursoHostname(f *testing.F) {
	testcases := []string{
		"my-db-myorg.turso.io",
		"my-db-myorg.turso.io:443",
		"singleword",
		"-leading-dash.turso.io",
		"trailing-dash-.turso.io",
		"nested-sub-domains.foo.bar.turso.io",
		"",
		"http://foo.bar",
	}
	for _, tc := range testcases {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, host string) {
		db, org, err := parseTursoHostname(host)
		if err == nil {
			if db == "" || org == "" {
				t.Errorf("expected non-empty db and org on success: host=%q", host)
			}
		}
	})
}

// FuzzResolveDriverAndDSN tests that driver resolution and DSN rewriting never panic on arbitrary input.
func FuzzResolveDriverAndDSN(f *testing.F) {
	testcases := []string{
		"dev.db",
		"file:test.db?cache=shared",
		"http://localhost:8080",
		"https://my-db.turso.io",
		"libsql://my-db.turso.io",
		"ws://127.0.0.1:8080",
		"wss://my-db.turso.io",
		"",
		":memory:",
		"   ",
		"file:///var/data/foo.db?_pragma=busy_timeout(1000)",
		"malformed://??&&--",
		"SELECT * FROM users;",
	}
	for _, tc := range testcases {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, rawURL string) {
		driver, dsn := ResolveDriverAndDSN(rawURL)
		if driver != "libsql" && driver != "sqlite" {
			t.Fatalf("unexpected driver %q", driver)
		}
		if dsn == "" {
			t.Fatalf("empty dsn returned for input %q", rawURL)
		}
	})
}

// FuzzValidateTopic tests that topic validation cleanly rejects invalid and SQL injection inputs.
func FuzzValidateTopic(f *testing.F) {
	testcases := []string{
		"valid_topic",
		"valid-topic-123",
		"Topic_Name",
		"",
		" ",
		"topic with spaces",
		"topic;DROP TABLE users;--",
		"topic' OR '1'='1",
		"a/b/c",
		"topic.with.dots",
		"very_long_topic_name_that_exceeds_sixty_four_characters_limit_by_a_wide_margin",
		"!@#$%^&*()",
	}
	for _, tc := range testcases {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, topic string) {
		err := validateTopic(topic)
		if err == nil {
			if len(topic) == 0 || len(topic) > 64 {
				t.Fatalf("accepted invalid length topic: %q", topic)
			}
			for _, r := range topic {
				isValidChar := (r >= 'a' && r <= 'z') ||
					(r >= 'A' && r <= 'Z') ||
					(r >= '0' && r <= '9') ||
					r == '_' || r == '-'
				if !isValidChar {
					t.Fatalf("accepted invalid character in topic: %q", topic)
				}
			}
		}
	})
}

// FuzzJSONDecoders tests that decoding arbitrary responses into node info and instances structures never crashes.
func FuzzJSONDecoders(f *testing.F) {
	testcases := []string{
		`{"role":"primary","replication_index":42}`,
		`{"role":"replica","upstream_url":"http://primary:8080"}`,
		`{"instances":[{"type":"primary","hostname":"db-primary.turso.io","region":"fra"}]}`,
		`{"instances":[]}`,
		`{}`,
		`null`,
		`[]`,
		`{"role":123}`,
		`{"instances":"not an array"}`,
		`{"role":"primary","unexpected":true}`,
		`not json at all`,
		`{"nested":{"deep":true}}`,
	}
	for _, tc := range testcases {
		f.Add([]byte(tc))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var node sqldNodeInfo
		_ = json.Unmarshal(data, &node)

		var instances tursoInstancesResponse
		_ = json.Unmarshal(data, &instances)
	})
}
