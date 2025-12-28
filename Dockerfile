# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build all binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/api ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/scheduler ./cmd/scheduler

# Worker image
FROM alpine:3.21 AS worker
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /bin/worker /usr/local/bin/worker
USER nobody:nobody
CMD ["worker"]

# API image
FROM alpine:3.21 AS api
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /bin/api /usr/local/bin/api
USER nobody:nobody
EXPOSE 8080
CMD ["api"]

# Scheduler image
FROM alpine:3.21 AS scheduler
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /bin/scheduler /usr/local/bin/scheduler
USER nobody:nobody
CMD ["scheduler"]
