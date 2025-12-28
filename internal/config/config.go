package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Redis     RedisConfig
	Database  DatabaseConfig
	Worker    WorkerConfig
	Scheduler SchedulerConfig
	API       APIConfig
	Job       JobConfig
	Log       LogConfig
}

type RedisConfig struct {
	URL          string        `mapstructure:"url"`
	MaxRetries   int           `mapstructure:"max_retries"`
	PoolSize     int           `mapstructure:"pool_size"`
	MinIdleConns int           `mapstructure:"min_idle_conns"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

type DatabaseConfig struct {
	URL             string        `mapstructure:"url"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`
}

type WorkerConfig struct {
	Concurrency     int           `mapstructure:"concurrency"`
	Queues          []string      `mapstructure:"queues"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	PollInterval    time.Duration `mapstructure:"poll_interval"`
}

type SchedulerConfig struct {
	PollInterval    time.Duration `mapstructure:"poll_interval"`
	BatchSize       int           `mapstructure:"batch_size"`
	StaleJobTimeout time.Duration `mapstructure:"stale_job_timeout"`
}

type APIConfig struct {
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout"`
}

type JobConfig struct {
	DefaultTimeout    time.Duration `mapstructure:"default_timeout"`
	DefaultMaxRetries int           `mapstructure:"default_max_retries"`
	RetryBaseDelay    time.Duration `mapstructure:"retry_base_delay"`
	RetryMaxDelay     time.Duration `mapstructure:"retry_max_delay"`
}

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func Load() (*Config, error) {
	v := viper.New()

	setDefaults(v)

	v.SetEnvPrefix("")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	bindEnvVars(v)

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("redis.url", "redis://localhost:6379")
	v.SetDefault("redis.max_retries", 3)
	v.SetDefault("redis.pool_size", 10)
	v.SetDefault("redis.min_idle_conns", 5)
	v.SetDefault("redis.dial_timeout", "5s")
	v.SetDefault("redis.read_timeout", "3s")
	v.SetDefault("redis.write_timeout", "3s")

	v.SetDefault("database.url", "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable")
	v.SetDefault("database.max_open_conns", 25)
	v.SetDefault("database.max_idle_conns", 5)
	v.SetDefault("database.conn_max_lifetime", "5m")
	v.SetDefault("database.conn_max_idle_time", "5m")

	v.SetDefault("worker.concurrency", 10)
	v.SetDefault("worker.queues", []string{"default"})
	v.SetDefault("worker.shutdown_timeout", "30s")
	v.SetDefault("worker.poll_interval", "5s")

	v.SetDefault("scheduler.poll_interval", "1s")
	v.SetDefault("scheduler.batch_size", 100)
	v.SetDefault("scheduler.stale_job_timeout", "5m")

	v.SetDefault("api.port", 8080)
	v.SetDefault("api.read_timeout", "15s")
	v.SetDefault("api.write_timeout", "15s")
	v.SetDefault("api.idle_timeout", "60s")

	v.SetDefault("job.default_timeout", "5m")
	v.SetDefault("job.default_max_retries", 3)
	v.SetDefault("job.retry_base_delay", "10s")
	v.SetDefault("job.retry_max_delay", "1h")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
}

func bindEnvVars(v *viper.Viper) {
	v.BindEnv("redis.url", "REDIS_URL")
	v.BindEnv("database.url", "DATABASE_URL")
	v.BindEnv("worker.concurrency", "WORKER_CONCURRENCY")
	v.BindEnv("worker.queues", "WORKER_QUEUES")
	v.BindEnv("worker.shutdown_timeout", "WORKER_SHUTDOWN_TIMEOUT")
	v.BindEnv("scheduler.poll_interval", "SCHEDULER_POLL_INTERVAL")
	v.BindEnv("api.port", "API_PORT")
	v.BindEnv("job.default_timeout", "JOB_DEFAULT_TIMEOUT")
	v.BindEnv("job.default_max_retries", "JOB_DEFAULT_MAX_RETRIES")
	v.BindEnv("job.retry_base_delay", "JOB_RETRY_BASE_DELAY")
	v.BindEnv("log.level", "LOG_LEVEL")
	v.BindEnv("log.format", "LOG_FORMAT")
}

func (c *Config) Validate() error {
	if c.Redis.URL == "" {
		return fmt.Errorf("redis.url is required")
	}
	if c.Database.URL == "" {
		return fmt.Errorf("database.url is required")
	}
	if c.Worker.Concurrency <= 0 {
		return fmt.Errorf("worker.concurrency must be positive")
	}
	if len(c.Worker.Queues) == 0 {
		return fmt.Errorf("worker.queues must not be empty")
	}
	if c.API.Port <= 0 || c.API.Port > 65535 {
		return fmt.Errorf("api.port must be between 1 and 65535")
	}
	return nil
}

func ParseRedisURL(url string) (*RedisOptions, error) {
	return &RedisOptions{
		URL: url,
	}, nil
}

type RedisOptions struct {
	URL string
}
