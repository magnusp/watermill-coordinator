# Watermill Publisher Coordinator & Ingress Gateway

> [!WARNING]
> **Disclaimer: AI-Generated Code**  
> This codebase, including the architecture, implementation, tests, and documentation, was generated and refined with AI assistance. Review, audit, and benchmark thoroughly before deploying in production environments.

A resilient, embeddable, pure-gRPC coordinator and ingress service built on [ThreeDotsLabs/Watermill](https://github.com/ThreeDotsLabs/watermill) with SQLite / [libSQL (`sqld`)](https://github.com/tursodatabase/libsql).

The coordinator provides **idempotent outbox ingestion** (e.g. for Spring Modulith / transactional outbox runners) and **bi-directional streaming subscriptions with client-side acknowledgments**, ensuring strict distributed safety and passing Jepsen fault-injection testing.

---

## Key Features

1. **Pure gRPC Architecture**: 
   - No HTTP overhead; strongly-typed Protocol Buffers definition ([`proto/coordinator.proto`](proto/coordinator.proto)) supporting Java, Go, Python, and other gRPC client code generation.
   - Built-in gRPC Server Reflection (`grpcurl` ready).
2. **Strict Idempotency & Deduplication**:
   - Outbox replays with identical `message_id` values hit a SQLite `UNIQUE INDEX ON uuid`.
   - Repeated publishes return `ALREADY_EXISTS` without duplicate inserts, data corruption, or offset disruption.
3. **Bi-Directional Streaming & Bounded Flow Control**:
   - Consumers explicitly acknowledge events via stream `AckRequest` frames before database offsets advance.
   - Includes a configurable sliding window prefetch limit (`MaxInFlight: 50`) to prevent heap exhaustion or OOM crashes under slow or stalled consumers.
4. **Three Mutually Exclusive Database Profiles**:
   - **Local SQLite (`modernc.org/sqlite`)**: Pure-Go zero-dependency local file or in-memory database with safe WAL mode pragmas.
   - **Self-Hosted `sqld` Cluster**: Supports asserting node primary role via the administrative API (`/v1/nodes`).
   - **Turso Cloud Hosted**: Automatic primary instance discovery via the Turso Management API, rewriting Anycast URLs to the location-specific primary hostname.
5. **Embeddable Library**:
   - Core functionality is cleanly isolated under `pkg/coordinator`, allowing it to be directly embedded into existing Go binaries, microservices, or sidecars.

---

## Architectural Findings & Distributed Safety (Jepsen Analysis)

During adversarial reliability reviews and Jepsen test modeling, critical distributed edge cases were evaluated and addressed:

### 1. The At-Most-Once Acknowledgment Trap
* **The Vulnerability**: In naive streaming implementations, the server commits the message offset immediately upon writing the TCP packet to the network buffer (`stream.Send`). If the consumer crashes (`SIGKILL`) or suffers a network partition before reading from its local socket, **messages are permanently lost**.
* **The Resolution**: Upgraded `Subscribe` to **bi-directional streaming**. The coordinator holds each yielded message in an in-flight unacknowledged state until the client returns an explicit `AckRequest(message_id)`. If the connection breaks before an ack is received, all in-flight messages are safely nacked/released and re-delivered upon reconnect.

### 2. Slow Consumer Heap Exhaustion (OOM)
* **The Vulnerability**: If a subscriber process stalls (e.g., JVM Stop-the-World GC pause or downstream database deadlock) while the coordinator continuously reads messages from SQLite, the in-flight buffer grows unbounded, eventually triggering an out-of-memory crash.
* **The Resolution**: Implemented **bounded flow control**. When `len(pendingAcks) >= MaxInFlight`, the coordinator pauses reading from the database channel until client acks free up capacity in the sliding window.

### 3. Local SQLite Concurrency Contention (`SQLITE_BUSY`)
* **The Vulnerability**: Even in WAL mode, SQLite permits only one concurrent writer. High concurrent publishing alongside subscriber ack updates can trigger `database is locked` errors if connection pools exceed writer limits.
* **The Resolution**: For local SQLite (`driver="sqlite"`), connection limits are automatically locked to `MaxOpenConns(1)` with safe PRAGMAs (`journal_mode=WAL`, `busy_timeout=5000`, `synchronous=NORMAL`).

### 4. Primary Writer Guarantee in Clustered `sqld`
* **The Issue**: In clustered `sqld` (libSQL), replicas transparently forward write transactions to the primary over internal gRPC. However, reading from a lagging replica without consistency tokens can violate read-your-own-writes guarantees. Furthermore, at the wire/SQL protocol level, `sqld` intentionally makes replicas behave identically to primaries (accepting writes and returning identical PRAGMAs).
* **The Resolution**:
  * **Self-Hosted**: If `SQLD_ADMIN_URL` is set, the coordinator verifies `GET /v1/nodes` to confirm `"role": "primary"`.
  * **Turso Cloud**: If targeting `*.turso.io`, the coordinator requires `TURSO_API_TOKEN` to query the Turso Platform API, discovering the primary region (e.g. `fra`) and pinning the connection string directly to `my-db-<region>-<org>.turso.io`.

---

## Configuration & Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `GRPC_PORT` | `:50051` | TCP port for the gRPC ingress service (falls back to `PORT`). |
| `DATABASE_URL` | `dev.db` | Target database DSN or file path. |
| `SQLD_ADMIN_URL` | `""` | Optional admin API endpoint for self-hosted `sqld` role verification. |
| `SQLD_ADMIN_AUTH` | `""` | Optional bearer token for self-hosted `sqld` admin API. |
| `TURSO_API_TOKEN` | `""` | Required for Turso Cloud (`*.turso.io`) to query management API and discover primary host. |
| `TURSO_ORG` | `""` | Optional Turso organization slug override. |
| `WRITE_TIMEOUT` | `5s` | Timeout for publish write operations. |
| `SHUTDOWN_TIMEOUT`| `5s` | Graceful shutdown timeout for active gRPC streams and transactions. |

---

## Quickstart

### Running as a Standalone Binary

```bash
# Local development (Zero Docker, pure-Go SQLite file: dev.db)
go run ./cmd/coordinator

# Against self-hosted sqld with admin role verification
DATABASE_URL="http://127.0.0.1:8080" \
SQLD_ADMIN_URL="http://127.0.0.1:9090" \
go run ./cmd/coordinator

# Against Turso Cloud
DATABASE_URL="libsql://my-db-myorg.turso.io?authToken=..." \
TURSO_API_TOKEN="turso_plat_..." \
go run ./cmd/coordinator
```

### Inspecting with `grpcurl`

```bash
# Check service health
grpcurl -plaintext localhost:50051 coordinator.CoordinatorService/CheckHealth

# Publish an event
grpcurl -plaintext -d '{
  "topic": "orders",
  "message_id": "ord-101",
  "payload": "eyJrZXkiOiAiZGF0YSJ9"
}' localhost:50051 coordinator.CoordinatorService/Publish

# Duplicate publish with same message_id (Idempotent ALREADY_EXISTS response)
grpcurl -plaintext -d '{
  "topic": "orders",
  "message_id": "ord-101",
  "payload": "eyJrZXkiOiAiZGF0YSJ9"
}' localhost:50051 coordinator.CoordinatorService/Publish
```

---

## Embedding in Another Go Application

Add the module to your application:

```go
package main

import (
	"log"
	"net"
	"time"

	"github.com/magnusp/watermill-coordinator/pkg/coordinator"
	"google.golang.org/grpc"
)

func main() {
	// 1. Initialize DB with profile verification
	db, err := coordinator.InitDB("dev.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 2. Instantiate embeddable coordinator service
	svc, err := coordinator.NewService(db, coordinator.ServiceOptions{
		WriteTimeout: 5 * time.Second,
		MaxInFlight:  50, // Sliding window prefetch limit
	})
	if err != nil {
		log.Fatal(err)
	}
	defer svc.Close()

	// 3. Register onto existing gRPC server
	server := grpc.NewServer()
	svc.Register(server)

	lis, _ := net.Listen("tcp", ":50051")
	server.Serve(lis)
}
```

---

## Running Test Suite

Run the full unit, flow control, and Jepsen crash simulation test suite:

```bash
go test -v ./...
```
