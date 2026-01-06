package mcp

import (
	"context"
	"errors"
	"testing"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/abdul-hamid-achik/job-queue/testutil"
	"github.com/mark3labs/mcp-go/mcp"
)

func TestServer_HandleEnqueueJob(t *testing.T) {
	t.Run("enqueues job successfully", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"type":    "email.send",
			"payload": map[string]string{"to": "test@example.com"},
		}

		result, err := s.handleEnqueueJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.IsError {
			t.Errorf("expected success, got error")
		}

		if len(broker.QueuedJobs()) != 1 {
			t.Errorf("expected 1 queued job, got %d", len(broker.QueuedJobs()))
		}
	})

	t.Run("enqueues job with options", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"type":        "payment.process",
			"queue":       "critical",
			"priority":    "high",
			"max_retries": float64(5),
			"delay":       "1h",
		}

		result, err := s.handleEnqueueJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.IsError {
			t.Errorf("expected success, got error")
		}

		jobs := broker.QueuedJobs()
		if len(jobs) != 1 {
			t.Fatalf("expected 1 queued job, got %d", len(jobs))
		}
	})

	t.Run("returns error when type is missing", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"payload": map[string]string{"to": "test@example.com"},
		}

		result, err := s.handleEnqueueJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})

	t.Run("returns error when enqueue fails", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		broker.EnqueueFunc = func(ctx context.Context, j *job.Job) error {
			return errors.New("redis connection lost")
		}
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"type": "email.send",
		}

		result, err := s.handleEnqueueJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleGetJob(t *testing.T) {
	t.Run("gets job successfully", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		testJob := testutil.NewTestJob()
		broker.Enqueue(context.Background(), testJob)

		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"id": testJob.ID,
		}

		result, err := s.handleGetJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.IsError {
			t.Errorf("expected success, got error")
		}
	})

	t.Run("returns error when id is missing", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{}

		result, err := s.handleGetJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})

	t.Run("returns error when job not found", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"id": "non-existent-id",
		}

		result, err := s.handleGetJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleDeleteJob(t *testing.T) {
	t.Run("deletes job successfully", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		testJob := testutil.NewTestJob()
		broker.Enqueue(context.Background(), testJob)

		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"id": testJob.ID,
		}

		result, err := s.handleDeleteJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.IsError {
			t.Errorf("expected success, got error")
		}
	})

	t.Run("returns error when id is missing", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{}

		result, err := s.handleDeleteJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleListQueues(t *testing.T) {
	t.Run("lists queues successfully", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}

		result, err := s.handleListQueues(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.IsError {
			t.Errorf("expected success, got error")
		}
	})
}

func TestServer_HandleGetQueueDepth(t *testing.T) {
	t.Run("gets queue depth successfully", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		// Enqueue some jobs
		for i := 0; i < 3; i++ {
			broker.Enqueue(context.Background(), testutil.NewTestJob())
		}

		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"name": "default",
		}

		result, err := s.handleGetQueueDepth(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.IsError {
			t.Errorf("expected success, got error")
		}
	})

	t.Run("returns error when name is missing", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{}

		result, err := s.handleGetQueueDepth(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})

	t.Run("returns error when broker fails", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		broker.GetQueueDepthFunc = func(ctx context.Context, queue string) (int64, error) {
			return 0, errors.New("redis error")
		}
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"name": "default",
		}

		result, err := s.handleGetQueueDepth(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleListDLQ(t *testing.T) {
	t.Run("returns error when dlqRepo is nil", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}

		result, err := s.handleListDLQ(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleGetDLQJob(t *testing.T) {
	t.Run("returns error when dlqRepo is nil", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"id": "test-id",
		}

		result, err := s.handleGetDLQJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})

	t.Run("returns error when id is missing", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{}

		result, err := s.handleGetDLQJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleRetryDLQJob(t *testing.T) {
	t.Run("returns error when dlqRepo is nil", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"id": "test-id",
		}

		result, err := s.handleRetryDLQJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})

	t.Run("returns error when id is missing", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{}

		result, err := s.handleRetryDLQJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleDeleteDLQJob(t *testing.T) {
	t.Run("returns error when dlqRepo is nil", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"id": "test-id",
		}

		result, err := s.handleDeleteDLQJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})

	t.Run("returns error when id is missing", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{}

		result, err := s.handleDeleteDLQJob(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleListExecutions(t *testing.T) {
	t.Run("returns error when execRepo is nil", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}

		result, err := s.handleListExecutions(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleGetJobExecutions(t *testing.T) {
	t.Run("returns error when execRepo is nil", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{
			"job_id": "test-id",
		}

		result, err := s.handleGetJobExecutions(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})

	t.Run("returns error when job_id is missing", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}
		request.Params.Arguments = map[string]interface{}{}

		result, err := s.handleGetJobExecutions(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleGetStats(t *testing.T) {
	t.Run("returns error when repos are nil", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}

		result, err := s.handleGetStats(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.IsError {
			t.Errorf("expected error result")
		}
	})
}

func TestServer_HandleHealthCheck(t *testing.T) {
	t.Run("returns healthy when broker is ok", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}

		result, err := s.handleHealthCheck(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.IsError {
			t.Errorf("expected success, got error")
		}
	})

	t.Run("returns result even when broker fails", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		broker.PingFunc = func(ctx context.Context) error {
			return errors.New("connection refused")
		}
		s := NewServer(broker, nil, nil)

		request := mcp.CallToolRequest{}

		result, err := s.handleHealthCheck(context.Background(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Health check still returns a result with status info, not an error
		if result.IsError {
			t.Errorf("health check should return result even when unhealthy")
		}
	})
}

func TestNewServer(t *testing.T) {
	t.Run("creates server with all dependencies", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		s := NewServer(broker, nil, nil)

		if s == nil {
			t.Fatal("expected non-nil server")
		}
		if s.broker == nil {
			t.Error("expected broker to be set")
		}
		if s.mcpServer == nil {
			t.Error("expected mcpServer to be set")
		}
	})
}
