package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
)

// ResolveDriverAndDSN inspects the database URL to determine whether to use the
// "libsql" driver (remote HTTP/libsql/WS endpoints) or the "sqlite" driver (modernc pure Go).
func ResolveDriverAndDSN(dbURL string) (driverName string, dsn string) {
	trimmed := strings.TrimSpace(dbURL)

	// Check for remote URL schemes
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "http://") ||
		strings.HasPrefix(lower, "https://") ||
		strings.HasPrefix(lower, "libsql://") ||
		strings.HasPrefix(lower, "ws://") ||
		strings.HasPrefix(lower, "wss://") {
		return "libsql", trimmed
	}

	// Local SQLite file, URI, or in-memory
	// Apply safe production pragmas for modernc.org/sqlite:
	// - journal_mode(WAL): Concurrent readers and single writer without blocking readers
	// - busy_timeout(5000): Wait up to 5s on locked database instead of failing immediately
	// - synchronous(NORMAL): Safe with WAL, eliminates unnecessary fsync overhead
	// - foreign_keys(ON): Enforce referential integrity
	// - cache_size(-20000): Allocate ~20MB page cache
	// - temp_store(MEMORY): Store temporary tables and indices in memory
	dsn = trimmed
	if dsn == "" {
		dsn = "dev.db"
	}

	pragmas := strings.Join([]string{
		"_pragma=journal_mode(WAL)",
		"_pragma=busy_timeout(5000)",
		"_pragma=synchronous(NORMAL)",
		"_pragma=foreign_keys(ON)",
		"_pragma=cache_size(-20000)",
		"_pragma=temp_store(MEMORY)",
	}, "&")

	if strings.Contains(dsn, "?") {
		dsn = dsn + "&" + pragmas
	} else {
		dsn = dsn + "?" + pragmas
	}

	return "sqlite", dsn
}

// DBOptions configures database initialization and profile-specific verification.
type DBOptions struct {
	SQLDAdminURL  string
	SQLDAdminAuth string
	TursoAPIToken string
	TursoOrg      string
}

// InitDB initializes and verifies the database connection based on the target profile:
// 1. Local SQLite: Uses pure Go modernc driver with safe WAL pragmas (admin/API checks skipped).
// 2. Self-Hosted sqld: If SQLDAdminURL is specified, asserts node is primary via /v1/nodes.
// 3. Turso Cloud: If connecting to *.turso.io, requires TursoAPIToken to verify/rewrite to primary hostname.
func InitDB(dbURL string, opts ...DBOptions) (*sql.DB, error) {
	driver, dsn := ResolveDriverAndDSN(dbURL)

	var opt DBOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	// Remote profile checks (libSQL)
	if driver == "libsql" {
		isTursoCloud := isTursoCloudHost(dbURL)

		if isTursoCloud {
			// Profile 3: Turso Cloud Hosted
			if opt.TursoAPIToken == "" {
				return nil, fmt.Errorf("turso cloud database detected (%s), but TURSO_API_TOKEN is required to guarantee primary writer routing", SanitizeDSN(dbURL))
			}

			rewrittenDSN, err := ResolveTursoPrimaryDSN(dbURL, opt.TursoAPIToken, opt.TursoOrg)
			if err != nil {
				return nil, fmt.Errorf("resolve turso cloud primary: %w", err)
			}
			dsn = rewrittenDSN
			log.Printf("[DB] Turso Cloud: resolved and verified primary endpoint: %s", SanitizeDSN(dsn))
		} else if opt.SQLDAdminURL != "" {
			// Profile 2: Self-hosted sqld with Admin API
			if err := AssertSqldPrimaryNode(opt.SQLDAdminURL, opt.SQLDAdminAuth); err != nil {
				return nil, fmt.Errorf("self-hosted sqld role assertion failed: %w", err)
			}
		}
	} else {
		// Profile 1: Local SQLite (pure-Go zero dependency)
		log.Printf("[DB] Local SQLite profile: admin and cloud assertions skipped")
	}

	log.Printf("[DB] Connecting with driver=%q, target=%q", driver, SanitizeDSN(dsn))

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open db (%s): %w", driver, err)
	}

	// Connection pool settings:
	// For local SQLite with modernc, limit max open connections to 1 writer connection
	// to eliminate busy timeout lock contention between concurrent goroutines.
	// For remote libsql/sqld, connection concurrency is handled by the remote server.
	if driver == "sqlite" {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	} else {
		db.SetMaxOpenConns(20)
		db.SetMaxIdleConns(10)
	}
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db (%s): %w", driver, err)
	}

	log.Printf("[DB] Connected successfully using driver %q", driver)
	return db, nil
}

type sqldNodeInfo struct {
	Role       string `json:"role"`
	PrimaryURL string `json:"primary_url,omitempty"`
}

// AssertSqldPrimaryNode queries sqld's administrative /v1/nodes endpoint to verify the node is a primary
func AssertSqldPrimaryNode(adminURL string, authKey string) error {
	trimmed := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	if trimmed == "" {
		return nil
	}

	endpoint := trimmed + "/v1/nodes"
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("create admin request: %w", err)
	}

	if authKey != "" {
		req.Header.Set("Authorization", "Bearer "+authKey)
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("query admin endpoint %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("admin endpoint returned HTTP %d", resp.StatusCode)
	}

	var node sqldNodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
		return fmt.Errorf("decode /v1/nodes response: %w", err)
	}

	if strings.ToLower(node.Role) != "primary" {
		if node.PrimaryURL != "" {
			return fmt.Errorf("node role is %q (syncing from %s), but coordinator strictly requires a primary writer", node.Role, node.PrimaryURL)
		}
		return fmt.Errorf("node role is %q, but coordinator strictly requires a primary writer", node.Role)
	}

	log.Printf("[DB] Successfully asserted sqld node at %s is primary", adminURL)
	return nil
}

// isTursoCloudHost checks if the target database URL points to Turso cloud (*.turso.io)
func isTursoCloudHost(dbURL string) bool {
	u, err := url.Parse(dbURL)
	if err != nil {
		return strings.Contains(strings.ToLower(dbURL), ".turso.io")
	}
	host := strings.ToLower(u.Host)
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}
	return strings.HasSuffix(host, ".turso.io")
}

// parseTursoHostname extracts the database name and org slug from a turso.io hostname
// e.g., "my-db-myorg.turso.io" -> dbName: "my-db", orgSlug: "myorg"
func parseTursoHostname(hostname string) (dbName, orgSlug string, err error) {
	host := strings.ToLower(hostname)
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	prefix := strings.TrimSuffix(host, ".turso.io")
	if prefix == host {
		return "", "", fmt.Errorf("not a valid turso.io hostname: %q", hostname)
	}

	lastHyphen := strings.LastIndex(prefix, "-")
	if lastHyphen == -1 || lastHyphen == 0 || lastHyphen == len(prefix)-1 {
		return "", "", fmt.Errorf("unable to parse database and organization from turso hostname %q", hostname)
	}

	dbName = prefix[:lastHyphen]
	orgSlug = prefix[lastHyphen+1:]
	return dbName, orgSlug, nil
}

type tursoInstanceItem struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Hostname string `json:"hostname"`
	Region   string `json:"region"`
}

type tursoInstancesResponse struct {
	Instances []tursoInstanceItem `json:"instances"`
}

// ResolveTursoPrimaryDSN queries the Turso Management API to discover the primary instance hostname
// and rewrites the database URL to point directly to that primary instance.
func ResolveTursoPrimaryDSN(dbURL, apiToken, overrideOrg string) (string, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return "", fmt.Errorf("parse db url: %w", err)
	}

	parsedDB, parsedOrg, err := parseTursoHostname(u.Host)
	if err != nil {
		return "", fmt.Errorf("parse turso host: %w", err)
	}

	orgSlug := parsedOrg
	if overrideOrg != "" {
		orgSlug = overrideOrg
	}
	dbName := parsedDB

	apiEndpoint := fmt.Sprintf("https://api.turso.tech/v1/organizations/%s/databases/%s/instances", orgSlug, dbName)
	req, err := http.NewRequest(http.MethodGet, apiEndpoint, nil)
	if err != nil {
		return "", fmt.Errorf("create turso api request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("query turso api (%s): %w", apiEndpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("turso api returned HTTP %d for %s", resp.StatusCode, apiEndpoint)
	}

	var instancesResp tursoInstancesResponse
	if err := json.NewDecoder(resp.Body).Decode(&instancesResp); err != nil {
		return "", fmt.Errorf("decode turso instances response: %w", err)
	}

	var primaryHost string
	for _, inst := range instancesResp.Instances {
		if strings.ToLower(inst.Type) == "primary" {
			primaryHost = inst.Hostname
			break
		}
	}

	if primaryHost == "" {
		return "", fmt.Errorf("no primary instance found in Turso API response for db %q in org %q", dbName, orgSlug)
	}

	// Preserve port if specified
	origHost := u.Host
	if idx := strings.Index(origHost, ":"); idx != -1 {
		primaryHost = primaryHost + origHost[idx:]
	}

	u.Host = primaryHost
	return u.String(), nil
}

// SanitizeDSN redacts credentials from DSN for safe logging.
func SanitizeDSN(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	return u.Redacted()
}
