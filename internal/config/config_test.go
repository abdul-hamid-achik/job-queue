package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear any existing env vars that might interfere
	envVars := []string{
		"REDIS_URL", "DATABASE_URL", "WORKER_CONCURRENCY",
		"API_PORT", "LOG_LEVEL", "LOG_FORMAT",
	}
	for _, env := range envVars {
		os.Unsetenv(env)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Check Redis defaults
	if cfg.Redis.URL != "redis://localhost:6379" {
		t.Errorf("expected default redis URL, got %s", cfg.Redis.URL)
	}
	if cfg.Redis.MaxRetries != 3 {
		t.Errorf("expected redis max_retries 3, got %d", cfg.Redis.MaxRetries)
	}
	if cfg.Redis.PoolSize != 10 {
		t.Errorf("expected redis pool_size 10, got %d", cfg.Redis.PoolSize)
	}

	// Check Database defaults
	if cfg.Database.URL != "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable" {
		t.Errorf("expected default database URL, got %s", cfg.Database.URL)
	}
	if cfg.Database.MaxOpenConns != 25 {
		t.Errorf("expected database max_open_conns 25, got %d", cfg.Database.MaxOpenConns)
	}

	// Check Worker defaults
	if cfg.Worker.Concurrency != 10 {
		t.Errorf("expected worker concurrency 10, got %d", cfg.Worker.Concurrency)
	}
	if len(cfg.Worker.Queues) != 1 || cfg.Worker.Queues[0] != "default" {
		t.Errorf("expected worker queues [default], got %v", cfg.Worker.Queues)
	}
	if cfg.Worker.ShutdownTimeout != 30*time.Second {
		t.Errorf("expected worker shutdown_timeout 30s, got %v", cfg.Worker.ShutdownTimeout)
	}

	// Check Scheduler defaults
	if cfg.Scheduler.PollInterval != 1*time.Second {
		t.Errorf("expected scheduler poll_interval 1s, got %v", cfg.Scheduler.PollInterval)
	}
	if cfg.Scheduler.BatchSize != 100 {
		t.Errorf("expected scheduler batch_size 100, got %d", cfg.Scheduler.BatchSize)
	}

	// Check API defaults
	if cfg.API.Port != 8080 {
		t.Errorf("expected api port 8080, got %d", cfg.API.Port)
	}
	if cfg.API.ReadTimeout != 15*time.Second {
		t.Errorf("expected api read_timeout 15s, got %v", cfg.API.ReadTimeout)
	}

	// Check Job defaults
	if cfg.Job.DefaultTimeout != 5*time.Minute {
		t.Errorf("expected job default_timeout 5m, got %v", cfg.Job.DefaultTimeout)
	}
	if cfg.Job.DefaultMaxRetries != 3 {
		t.Errorf("expected job default_max_retries 3, got %d", cfg.Job.DefaultMaxRetries)
	}

	// Check Log defaults
	if cfg.Log.Level != "info" {
		t.Errorf("expected log level info, got %s", cfg.Log.Level)
	}
	if cfg.Log.Format != "json" {
		t.Errorf("expected log format json, got %s", cfg.Log.Format)
	}
}

func TestLoad_FromEnv(t *testing.T) {
	// Set environment variables
	os.Setenv("REDIS_URL", "redis://custom:6380")
	os.Setenv("DATABASE_URL", "postgres://user:pass@db:5432/test")
	os.Setenv("WORKER_CONCURRENCY", "20")
	os.Setenv("API_PORT", "9090")
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("LOG_FORMAT", "console")

	defer func() {
		os.Unsetenv("REDIS_URL")
		os.Unsetenv("DATABASE_URL")
		os.Unsetenv("WORKER_CONCURRENCY")
		os.Unsetenv("API_PORT")
		os.Unsetenv("LOG_LEVEL")
		os.Unsetenv("LOG_FORMAT")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Redis.URL != "redis://custom:6380" {
		t.Errorf("expected redis URL from env, got %s", cfg.Redis.URL)
	}
	if cfg.Database.URL != "postgres://user:pass@db:5432/test" {
		t.Errorf("expected database URL from env, got %s", cfg.Database.URL)
	}
	if cfg.Worker.Concurrency != 20 {
		t.Errorf("expected worker concurrency 20, got %d", cfg.Worker.Concurrency)
	}
	if cfg.API.Port != 9090 {
		t.Errorf("expected api port 9090, got %d", cfg.API.Port)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("expected log level debug, got %s", cfg.Log.Level)
	}
	if cfg.Log.Format != "console" {
		t.Errorf("expected log format console, got %s", cfg.Log.Format)
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: Config{
				Redis:    RedisConfig{URL: "redis://localhost:6379"},
				Database: DatabaseConfig{URL: "postgres://localhost/db"},
				Worker:   WorkerConfig{Concurrency: 10, Queues: []string{"default"}},
				API:      APIConfig{Port: 8080},
			},
			wantErr: false,
		},
		{
			name: "missing redis URL",
			config: Config{
				Redis:    RedisConfig{URL: ""},
				Database: DatabaseConfig{URL: "postgres://localhost/db"},
				Worker:   WorkerConfig{Concurrency: 10, Queues: []string{"default"}},
				API:      APIConfig{Port: 8080},
			},
			wantErr: true,
			errMsg:  "redis.url is required",
		},
		{
			name: "missing database URL",
			config: Config{
				Redis:    RedisConfig{URL: "redis://localhost:6379"},
				Database: DatabaseConfig{URL: ""},
				Worker:   WorkerConfig{Concurrency: 10, Queues: []string{"default"}},
				API:      APIConfig{Port: 8080},
			},
			wantErr: true,
			errMsg:  "database.url is required",
		},
		{
			name: "zero concurrency",
			config: Config{
				Redis:    RedisConfig{URL: "redis://localhost:6379"},
				Database: DatabaseConfig{URL: "postgres://localhost/db"},
				Worker:   WorkerConfig{Concurrency: 0, Queues: []string{"default"}},
				API:      APIConfig{Port: 8080},
			},
			wantErr: true,
			errMsg:  "worker.concurrency must be positive",
		},
		{
			name: "negative concurrency",
			config: Config{
				Redis:    RedisConfig{URL: "redis://localhost:6379"},
				Database: DatabaseConfig{URL: "postgres://localhost/db"},
				Worker:   WorkerConfig{Concurrency: -1, Queues: []string{"default"}},
				API:      APIConfig{Port: 8080},
			},
			wantErr: true,
			errMsg:  "worker.concurrency must be positive",
		},
		{
			name: "empty queues",
			config: Config{
				Redis:    RedisConfig{URL: "redis://localhost:6379"},
				Database: DatabaseConfig{URL: "postgres://localhost/db"},
				Worker:   WorkerConfig{Concurrency: 10, Queues: []string{}},
				API:      APIConfig{Port: 8080},
			},
			wantErr: true,
			errMsg:  "worker.queues must not be empty",
		},
		{
			name: "invalid port zero",
			config: Config{
				Redis:    RedisConfig{URL: "redis://localhost:6379"},
				Database: DatabaseConfig{URL: "postgres://localhost/db"},
				Worker:   WorkerConfig{Concurrency: 10, Queues: []string{"default"}},
				API:      APIConfig{Port: 0},
			},
			wantErr: true,
			errMsg:  "api.port must be between 1 and 65535",
		},
		{
			name: "invalid port too high",
			config: Config{
				Redis:    RedisConfig{URL: "redis://localhost:6379"},
				Database: DatabaseConfig{URL: "postgres://localhost/db"},
				Worker:   WorkerConfig{Concurrency: 10, Queues: []string{"default"}},
				API:      APIConfig{Port: 70000},
			},
			wantErr: true,
			errMsg:  "api.port must be between 1 and 65535",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				} else if err.Error() != tt.errMsg {
					t.Errorf("expected error '%s', got '%s'", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestParseRedisURL(t *testing.T) {
	opts, err := ParseRedisURL("redis://localhost:6379")
	if err != nil {
		t.Fatalf("ParseRedisURL failed: %v", err)
	}

	if opts.URL != "redis://localhost:6379" {
		t.Errorf("expected URL to be set, got %s", opts.URL)
	}
}

func TestRedisConfig_Timeouts(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Redis.DialTimeout != 5*time.Second {
		t.Errorf("expected dial_timeout 5s, got %v", cfg.Redis.DialTimeout)
	}
	if cfg.Redis.ReadTimeout != 3*time.Second {
		t.Errorf("expected read_timeout 3s, got %v", cfg.Redis.ReadTimeout)
	}
	if cfg.Redis.WriteTimeout != 3*time.Second {
		t.Errorf("expected write_timeout 3s, got %v", cfg.Redis.WriteTimeout)
	}
}

func TestDatabaseConfig_ConnectionPool(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Database.MaxIdleConns != 5 {
		t.Errorf("expected max_idle_conns 5, got %d", cfg.Database.MaxIdleConns)
	}
	if cfg.Database.ConnMaxLifetime != 5*time.Minute {
		t.Errorf("expected conn_max_lifetime 5m, got %v", cfg.Database.ConnMaxLifetime)
	}
	if cfg.Database.ConnMaxIdleTime != 5*time.Minute {
		t.Errorf("expected conn_max_idle_time 5m, got %v", cfg.Database.ConnMaxIdleTime)
	}
}

func TestJobConfig_RetrySettings(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Job.RetryBaseDelay != 10*time.Second {
		t.Errorf("expected retry_base_delay 10s, got %v", cfg.Job.RetryBaseDelay)
	}
	if cfg.Job.RetryMaxDelay != 1*time.Hour {
		t.Errorf("expected retry_max_delay 1h, got %v", cfg.Job.RetryMaxDelay)
	}
}
