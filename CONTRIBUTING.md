# Contributing to Job Queue

Thank you for your interest in contributing to this project! This guide will help you get started.

## Getting Started

### Prerequisites

- Go 1.23+
- Docker and Docker Compose
- Redis (for integration tests)
- PostgreSQL (for repository tests)
- [Task](https://taskfile.dev/) (recommended)

### Setup

```bash
# Clone the repository
git clone https://github.com/abdul-hamid-achik/job-queue.git
cd job-queue

# Start infrastructure
docker compose up -d redis db

# Run migrations
task migrate

# Verify setup
task test
```

## Development Workflow

### Running Tests

```bash
# Run all tests
task test

# Run with coverage
task test:coverage

# Run specific package tests
go test ./pkg/broker/...
```

### Code Quality

```bash
# Format code
task fmt

# Run linter
task lint

# Tidy dependencies
task tidy
```

## Code Style Guidelines

### Comments

- **Avoid unnecessary comments** - Code should be self-documenting
- Only add comments when explaining non-obvious logic or business rules
- Do not add comments that simply restate what the code does
- Remove TODO/FIXME comments once addressed
- Keep doc comments concise and focused on the "why" not the "what"

### Naming

- Use descriptive variable and function names
- Prefer clarity over brevity
- Follow Go naming conventions (camelCase for unexported, PascalCase for exported)

### Testing

- Tests should use Redis database 1 to avoid conflicts with running services
- Use unique consumer group names per test for isolation
- Use table-driven tests where appropriate
- Target 80%+ code coverage for all packages

## Project Structure

```
pkg/           - Public packages for external use
cmd/           - Application entry points
docs/          - Documentation
migrations/    - Database migrations
testutil/      - Test utilities and mocks
```

## Key Technical Decisions

### Redis Streams

- Consumer groups are created BEFORE messages are added to streams
- Use unique group names in tests to avoid conflicts with other services
- Tests use Redis DB 1, production uses DB 0

### Logging

- Use zerolog for structured logging
- Debug level for routine operations (enqueue, dequeue)
- Info level for lifecycle events (startup, shutdown, group creation)
- Warn level for retries
- Error level for failures

## Submitting Changes

### Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests and ensure they pass (`task test`)
5. Check coverage meets 80%+ threshold (`task test:coverage`)
6. Commit your changes with a clear message
7. Push to your fork
8. Open a Pull Request

### Commit Messages

Use clear, descriptive commit messages:

- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `test:` for test additions/changes
- `refactor:` for code refactoring
- `chore:` for maintenance tasks

Example:
```
feat: add support for job priorities in worker pool

- Implement priority-based queue selection
- Add WithPriority option to job creation
- Update worker to check high priority queues first
```

### Code Review

All submissions require review. We use GitHub pull requests for this purpose.

## Release Process

Releases are managed by maintainers:

1. Run all tests: `go test ./...`
2. Check coverage: `go test -cover ./pkg/...`
3. Update CHANGELOG.md
4. Update version in api/openapi.yaml
5. Commit changes
6. Create release: `gh release create vX.Y.Z --title "..." --notes "..."`

## Getting Help

- Open an issue for bugs or feature requests
- Check existing issues before creating new ones
- Provide clear reproduction steps for bugs

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
