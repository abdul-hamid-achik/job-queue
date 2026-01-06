# Agent Guidelines

Guidelines for AI agents working on this codebase.

## Code Style

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
- Target 80%+ code coverage

## Project Structure

```
pkg/           - Public packages for external use
cmd/           - Application entry points
docs/          - Documentation
migrations/    - Database migrations
testutil/      - Test utilities and mocks
```

## Key Decisions

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

## Release Process

1. Run all tests: `go test ./...`
2. Check coverage: `go test -cover ./pkg/...`
3. Update CHANGELOG.md
4. Update version in api/openapi.yaml
5. Commit changes
6. Create release: `gh release create vX.Y.Z --title "..." --notes "..."`
