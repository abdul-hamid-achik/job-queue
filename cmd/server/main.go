package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abdul-hamid-achik/job-queue/internal/broker"
	"github.com/abdul-hamid-achik/job-queue/internal/config"
	"github.com/abdul-hamid-achik/job-queue/internal/handler"
	"github.com/abdul-hamid-achik/job-queue/internal/repository"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	// Setup logger
	logger := setupLogger(cfg.Log)

	logger.Info().
		Int("port", cfg.API.Port).
		Msg("starting API server")

	// Create Redis client
	redisOpts, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to parse Redis URL")
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	// Ping Redis
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to Redis")
	}
	logger.Info().Msg("connected to Redis")

	// Create PostgreSQL connection pool
	dbPool, err := pgxpool.New(context.Background(), cfg.Database.URL)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create database pool")
	}
	defer dbPool.Close()

	// Ping database
	if err := dbPool.Ping(context.Background()); err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to database")
	}
	logger.Info().Msg("connected to PostgreSQL")

	// Create broker and repositories
	b := broker.NewRedisStreamsBroker(redisClient)
	execRepo := repository.NewExecutionRepository(dbPool)
	dlqRepo := repository.NewDLQRepository(dbPool)

	// Create API handler
	apiHandler := handler.NewAPIHandler(b, execRepo, dlqRepo, logger)

	// Create HTTP server
	mux := http.NewServeMux()
	apiHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.API.Port),
		Handler:      mux,
		ReadTimeout:  cfg.API.ReadTimeout,
		WriteTimeout: cfg.API.WriteTimeout,
		IdleTimeout:  cfg.API.IdleTimeout,
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", server.Addr).Msg("HTTP server listening")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		logger.Info().Str("signal", sig.String()).Msg("received shutdown signal")
	case err := <-errChan:
		logger.Error().Err(err).Msg("server error")
	case <-ctx.Done():
	}

	// Graceful shutdown
	logger.Info().Msg("initiating graceful shutdown")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("shutdown error")
		os.Exit(1)
	}

	logger.Info().Msg("API server shutdown complete")
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

	return logger.With().Str("component", "api").Logger()
}
