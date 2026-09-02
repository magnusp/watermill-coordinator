package coordinator

import (
	"os"
	"time"
)

type Config struct {
	GRPCPort        string
	DatabaseURL     string
	SQLDAdminURL    string
	SQLDAdminAuth   string
	TursoAPIToken   string
	TursoOrg        string
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return fallback
}

// LoadConfigFromEnv loads configuration from environment variables with sensible defaults.
func LoadConfigFromEnv() Config {
	port := getEnv("GRPC_PORT", getEnv("PORT", ":50051"))
	if len(port) > 0 && port[0] != ':' {
		port = ":" + port
	}

	return Config{
		GRPCPort:        port,
		DatabaseURL:     getEnv("DATABASE_URL", "dev.db"),
		SQLDAdminURL:    getEnv("SQLD_ADMIN_URL", ""),
		SQLDAdminAuth:   getEnv("SQLD_ADMIN_AUTH", getEnv("LIBSQL_ADMIN_AUTH_KEY", "")),
		TursoAPIToken:   getEnv("TURSO_API_TOKEN", ""),
		TursoOrg:        getEnv("TURSO_ORG", ""),
		WriteTimeout:    getEnvDuration("WRITE_TIMEOUT", 5*time.Second),
		ShutdownTimeout: getEnvDuration("SHUTDOWN_TIMEOUT", 5*time.Second),
	}
}
