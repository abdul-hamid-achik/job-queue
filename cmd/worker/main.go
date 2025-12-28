package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abdul-hamid-achik/job-queue/internal/broker"
	"github.com/abdul-hamid-achik/job-queue/internal/config"
	"github.com/abdul-hamid-achik/job-queue/internal/middleware"
	"github.com/abdul-hamid-achik/job-queue/internal/worker"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	logger := setupLogger(cfg.Log)
	logger.Info().
		Int("concurrency", cfg.Worker.Concurrency).
		Strs("queues", cfg.Worker.Queues).
		Msg("starting worker")

	redisOpts, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to parse Redis URL")
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to Redis")
	}
	logger.Info().Msg("connected to Redis")

	b := broker.NewRedisStreamsBroker(redisClient)
	registry := worker.NewRegistry()

	registry.Use(
		middleware.RecoveryMiddleware(logger),
		middleware.LoggingMiddleware(logger),
		middleware.TimeoutMiddleware(cfg.Job.DefaultTimeout),
	)

	pool := worker.NewPool(
		b,
		registry,
		worker.WithConcurrency(cfg.Worker.Concurrency),
		worker.WithPoolQueues(cfg.Worker.Queues),
		worker.WithShutdownTimeout(cfg.Worker.ShutdownTimeout),
		worker.WithPoolPollInterval(cfg.Worker.PollInterval),
		worker.WithPoolLogger(logger),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	errChan := make(chan error, 1)
	go func() {
		errChan <- pool.Start(ctx)
	}()

	select {
	case sig := <-sigChan:
		logger.Info().Str("signal", sig.String()).Msg("received shutdown signal")
	case err := <-errChan:
		if err != nil && err != context.Canceled {
			logger.Error().Err(err).Msg("pool error")
		}
	}

	logger.Info().Msg("initiating graceful shutdown")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Worker.ShutdownTimeout)
	defer shutdownCancel()

	if err := pool.Stop(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("shutdown error")
		os.Exit(1)
	}

	logger.Info().Msg("worker shutdown complete")
}

func setupLogger(cfg config.LogConfig) zerolog.Logger {
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	var logger zerolog.Logger
	if cfg.Format == "console" {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
			With().Timestamp().Caller().Logger()
	} else {
		logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}

	return logger.With().Str("component", "worker").Logger()
}
