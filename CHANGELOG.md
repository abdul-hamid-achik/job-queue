# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-01-06

### Added

- Comprehensive test suite improvements achieving 80%+ coverage on most packages:
  - handler: 51% → 97.4%
  - middleware: 48% → 95.1%
  - scheduler: 43% → 94.8%
  - job: 70% → 96.3%
  - mcp: 58% → 96.2%
  - broker: 60% → 80.2%
  - worker: 83.3%
  - config: 96.9%
- Mock repository implementations (`MockDLQRepository`, `MockExecutionRepository`, `MockScheduleRepository`) for comprehensive unit testing
- Interface-based repository design for better testability (`DLQRepository`, `ExecutionRepository`, `ScheduleRepository`)
- Full test coverage for job state transitions (`MarkStarted`, `MarkCompleted`, `MarkFailed`, `MarkDead`)
- CronScheduler comprehensive tests including error handling paths

### Changed

- Refactored `APIHandler` to use repository interfaces instead of concrete types
- Refactored `CronScheduler` to use `ScheduleRepository` interface
- Improved test isolation with unique IDs and cleanup

### Fixed

- Handler tests now properly mock DLQ and execution repositories
- Scheduler tests now properly mock schedule repository operations

## [0.4.0] - 2026-01-06

### Fixed

- **Critical**: Fixed race condition where consumer groups were created AFTER messages were added to streams, causing messages to be invisible to workers
- **Critical**: Reordered `Enqueue` to create consumer groups BEFORE adding messages to streams
- Fixed test isolation by using separate Redis database (DB 1) for integration tests
- Fixed test isolation by using unique consumer group names per test

### Added

- Structured logging throughout the broker layer using zerolog
  - Debug logs for enqueue/dequeue operations
  - Info logs for consumer group creation
  - Warn logs for job retries with backoff details
  - Error logs for Redis failures and dead jobs
- `WithLogger(zerolog.Logger)` option for `RedisStreamsBroker`
- Comprehensive MCP handler tests (58% coverage)
- Test helper `newTestBroker()` for isolated broker instances

### Changed

- Broker integration tests now use Redis database 1 to avoid conflicts with other running services
- Improved error messages in broker operations with more context

## [0.3.0] - 2024-12-28

### Added

- Exposed packages publicly via `pkg/` for external use
- MCP (Model Context Protocol) server for LLM integration
- OpenAPI 3.1 specification for HTTP API

## [0.2.1] - 2024-12-28

### Fixed

- Removed redundant comments from code

## [0.2.0] - 2024-12-28

### Added

- Comprehensive tests for config, handler, and scheduler packages
- Test utilities and mock broker for unit testing

## [0.1.0] - 2024-12-28

### Added

- Initial release
- Redis Streams broker with priority queues
- Worker pool with configurable concurrency
- Graceful shutdown support
- Retry with exponential backoff and jitter
- Dead Letter Queue (PostgreSQL)
- Delayed jobs and cron scheduling
- HTTP API for job management
- Execution history tracking
