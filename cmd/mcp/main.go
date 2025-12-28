package main

import (
	"context"
	"log"
	"os"

	"github.com/abdul-hamid-achik/job-queue/internal/broker"
	"github.com/abdul-hamid-achik/job-queue/internal/config"
	"github.com/abdul-hamid-achik/job-queue/internal/mcp"
	"github.com/abdul-hamid-achik/job-queue/internal/repository"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Create Redis client
	redisOpts, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		log.Fatalf("failed to parse Redis URL: %v", err)
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	// Ping Redis
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}

	// Create broker
	b := broker.NewRedisStreamsBroker(redisClient)

	// Try to create PostgreSQL connection (optional for MCP)
	var execRepo *repository.ExecutionRepository
	var dlqRepo *repository.DLQRepository

	if cfg.Database.URL != "" {
		dbPool, err := pgxpool.New(context.Background(), cfg.Database.URL)
		if err == nil {
			if err := dbPool.Ping(context.Background()); err == nil {
				execRepo = repository.NewExecutionRepository(dbPool)
				dlqRepo = repository.NewDLQRepository(dbPool)
				defer dbPool.Close()
			}
		}
	}

	// Create and start MCP server
	server := mcp.NewServer(b, execRepo, dlqRepo)

	// Serve via stdio
	if err := server.ServeStdio(); err != nil {
		log.Printf("MCP server error: %v", err)
		os.Exit(1)
	}
}
