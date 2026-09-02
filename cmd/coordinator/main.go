package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/magnusp/watermill-coordinator/pkg/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

func main() {
	cfg := coordinator.LoadConfigFromEnv()

	log.Printf("Starting Watermill gRPC Coordinator (version: %s, commit: %s, date: %s)...", Version, Commit, Date)
	log.Printf("[CONFIG] GRPC_PORT: %s", cfg.GRPCPort)
	log.Printf("[CONFIG] DATABASE_URL: %s", coordinator.SanitizeDSN(cfg.DatabaseURL))

	if cfg.SQLDAdminURL != "" {
		log.Printf("[CONFIG] SQLD_ADMIN_URL: %s", cfg.SQLDAdminURL)
	}
	if cfg.TursoAPIToken != "" {
		log.Printf("[CONFIG] TURSO_API_TOKEN is set (cloud primary auto-discovery enabled)")
	}

	db, err := coordinator.InitDB(cfg.DatabaseURL, coordinator.DBOptions{
		SQLDAdminURL:  cfg.SQLDAdminURL,
		SQLDAdminAuth: cfg.SQLDAdminAuth,
		TursoAPIToken: cfg.TursoAPIToken,
		TursoOrg:      cfg.TursoOrg,
	})
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer db.Close()

	wmLogger := watermill.NewStdLogger(false, false)

	service, err := coordinator.NewService(db, coordinator.ServiceOptions{
		WriteTimeout: cfg.WriteTimeout,
		Logger:       wmLogger,
	})
	if err != nil {
		log.Fatalf("Coordinator service initialization failed: %v", err)
	}
	defer service.Close()

	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", cfg.GRPCPort, err)
	}

	grpcServer := grpc.NewServer(
		grpc.ConnectionTimeout(cfg.WriteTimeout),
	)

	service.Register(grpcServer)
	reflection.Register(grpcServer)

	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("Coordinator gRPC service listening on %s", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			serverErrors <- err
		}
	}()

	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		log.Fatalf("gRPC server error: %v", err)
	case sig := <-shutdownSig:
		log.Printf("Signal %v received, initiating graceful shutdown...", sig)
	}

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Println("gRPC server stopped cleanly.")
	case <-time.After(cfg.ShutdownTimeout):
		log.Println("Graceful shutdown timed out, forcing stop.")
		grpcServer.Stop()
	}

	log.Println("Coordinator shutdown completed.")
}
