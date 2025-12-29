package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/abdul-hamid-achik/job-queue/pkg/repository"
	"github.com/mark3labs/mcp-go/mcp"
)

// handleEnqueueJob creates and enqueues a new job.
func (s *Server) handleEnqueueJob(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	jobType, err := request.RequireString("type")
	if err != nil {
		return mcp.NewToolResultError("job type is required"), nil
	}

	opts := []job.Option{}

	if queue := request.GetString("queue", ""); queue != "" {
		opts = append(opts, job.WithQueue(queue))
	}
	if priority := request.GetString("priority", ""); priority != "" {
		opts = append(opts, job.WithPriority(job.ParsePriority(priority)))
	}
	if maxRetries := request.GetInt("max_retries", 0); maxRetries > 0 {
		opts = append(opts, job.WithMaxRetries(maxRetries))
	}
	if delay := request.GetString("delay", ""); delay != "" {
		if d, err := time.ParseDuration(delay); err == nil {
			opts = append(opts, job.WithDelay(d))
		}
	}

	var payload interface{}
	args := request.GetArguments()
	if p, ok := args["payload"]; ok {
		payload = p
	}

	j, err := job.NewWithOptions(jobType, payload, opts...)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to create job: %v", err)), nil
	}

	if err := s.broker.Enqueue(ctx, j); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to enqueue job: %v", err)), nil
	}

	result, _ := json.MarshalIndent(j, "", "  ")
	return mcp.NewToolResultText(string(result)), nil
}

// handleGetJob retrieves a job by ID.
func (s *Server) handleGetJob(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := request.RequireString("id")
	if err != nil {
		return mcp.NewToolResultError("job ID is required"), nil
	}

	j, err := s.broker.GetJob(ctx, id)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get job: %v", err)), nil
	}

	result, _ := json.MarshalIndent(j, "", "  ")
	return mcp.NewToolResultText(string(result)), nil
}

// handleDeleteJob deletes a job by ID.
func (s *Server) handleDeleteJob(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := request.RequireString("id")
	if err != nil {
		return mcp.NewToolResultError("job ID is required"), nil
	}

	if err := s.broker.DeleteJob(ctx, id); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to delete job: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("Job %s deleted successfully", id)), nil
}

// handleListQueues lists all queues with depths.
func (s *Server) handleListQueues(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	queues := []string{"critical", "default", "low"}
	result := make([]map[string]interface{}, 0, len(queues))

	for _, queue := range queues {
		depth, _ := s.broker.GetQueueDepth(ctx, queue)
		result = append(result, map[string]interface{}{
			"name":  queue,
			"depth": depth,
		})
	}

	output, _ := json.MarshalIndent(result, "", "  ")
	return mcp.NewToolResultText(string(output)), nil
}

// handleGetQueueDepth gets the depth of a specific queue.
func (s *Server) handleGetQueueDepth(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	name, err := request.RequireString("name")
	if err != nil {
		return mcp.NewToolResultError("queue name is required"), nil
	}

	depth, err := s.broker.GetQueueDepth(ctx, name)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get queue depth: %v", err)), nil
	}

	result := map[string]interface{}{
		"queue": name,
		"depth": depth,
	}
	output, _ := json.MarshalIndent(result, "", "  ")
	return mcp.NewToolResultText(string(output)), nil
}

// handleListDLQ lists jobs in the dead letter queue.
func (s *Server) handleListDLQ(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if s.dlqRepo == nil {
		return mcp.NewToolResultError("DLQ repository not available"), nil
	}

	filter := repository.DLQFilter{
		Limit:  request.GetInt("limit", 100),
		Offset: request.GetInt("offset", 0),
	}

	if jobType := request.GetString("type", ""); jobType != "" {
		filter.JobType = &jobType
	}
	if queue := request.GetString("queue", ""); queue != "" {
		filter.Queue = &queue
	}

	jobs, err := s.dlqRepo.List(ctx, filter)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list DLQ: %v", err)), nil
	}

	output, _ := json.MarshalIndent(jobs, "", "  ")
	return mcp.NewToolResultText(string(output)), nil
}

// handleGetDLQJob gets a specific job from the DLQ.
func (s *Server) handleGetDLQJob(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if s.dlqRepo == nil {
		return mcp.NewToolResultError("DLQ repository not available"), nil
	}

	id, err := request.RequireString("id")
	if err != nil {
		return mcp.NewToolResultError("DLQ ID is required"), nil
	}

	dlj, err := s.dlqRepo.GetByID(ctx, id)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get DLQ job: %v", err)), nil
	}

	output, _ := json.MarshalIndent(dlj, "", "  ")
	return mcp.NewToolResultText(string(output)), nil
}

// handleRetryDLQJob retries a job from the DLQ.
func (s *Server) handleRetryDLQJob(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if s.dlqRepo == nil {
		return mcp.NewToolResultError("DLQ repository not available"), nil
	}

	id, err := request.RequireString("id")
	if err != nil {
		return mcp.NewToolResultError("DLQ ID is required"), nil
	}

	dlj, err := s.dlqRepo.GetByID(ctx, id)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get DLQ job: %v", err)), nil
	}

	j := dlj.ToJob()
	if err := s.broker.Enqueue(ctx, j); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to requeue job: %v", err)), nil
	}

	_ = s.dlqRepo.MarkRequeued(ctx, id)

	output, _ := json.MarshalIndent(j, "", "  ")
	return mcp.NewToolResultText(fmt.Sprintf("Job requeued successfully:\n%s", string(output))), nil
}

// handleDeleteDLQJob deletes a job from the DLQ.
func (s *Server) handleDeleteDLQJob(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if s.dlqRepo == nil {
		return mcp.NewToolResultError("DLQ repository not available"), nil
	}

	id, err := request.RequireString("id")
	if err != nil {
		return mcp.NewToolResultError("DLQ ID is required"), nil
	}

	if err := s.dlqRepo.Delete(ctx, id); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to delete DLQ job: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("DLQ job %s deleted successfully", id)), nil
}

// handleListExecutions lists job execution history.
func (s *Server) handleListExecutions(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if s.execRepo == nil {
		return mcp.NewToolResultError("execution repository not available"), nil
	}

	filter := repository.ExecutionFilter{
		Limit:  request.GetInt("limit", 100),
		Offset: request.GetInt("offset", 0),
	}

	if jobType := request.GetString("type", ""); jobType != "" {
		filter.JobType = &jobType
	}
	if state := request.GetString("state", ""); state != "" {
		filter.State = &state
	}

	executions, err := s.execRepo.List(ctx, filter)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list executions: %v", err)), nil
	}

	output, _ := json.MarshalIndent(executions, "", "  ")
	return mcp.NewToolResultText(string(output)), nil
}

// handleGetJobExecutions gets execution history for a specific job.
func (s *Server) handleGetJobExecutions(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if s.execRepo == nil {
		return mcp.NewToolResultError("execution repository not available"), nil
	}

	jobID, err := request.RequireString("job_id")
	if err != nil {
		return mcp.NewToolResultError("job_id is required"), nil
	}

	executions, err := s.execRepo.GetByJobID(ctx, jobID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get job executions: %v", err)), nil
	}

	output, _ := json.MarshalIndent(executions, "", "  ")
	return mcp.NewToolResultText(string(output)), nil
}

// handleGetStats gets overall statistics.
func (s *Server) handleGetStats(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if s.execRepo == nil || s.dlqRepo == nil {
		return mcp.NewToolResultError("repositories not available"), nil
	}

	now := time.Now()
	fromDate := now.Add(-24 * time.Hour)

	stats, err := s.execRepo.GetStats(ctx, fromDate, now)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get stats: %v", err)), nil
	}

	dlqCount, _ := s.dlqRepo.Count(ctx)

	result := map[string]interface{}{
		"executions_24h":    stats,
		"dead_letter_count": dlqCount,
	}

	output, _ := json.MarshalIndent(result, "", "  ")
	return mcp.NewToolResultText(string(output)), nil
}

// handleHealthCheck checks if the system is healthy.
func (s *Server) handleHealthCheck(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	status := map[string]interface{}{
		"status": "ok",
		"checks": map[string]string{},
	}
	checks := status["checks"].(map[string]string)

	if err := s.broker.Ping(ctx); err != nil {
		checks["redis"] = fmt.Sprintf("error: %v", err)
		status["status"] = "unhealthy"
	} else {
		checks["redis"] = "ok"
	}

	output, _ := json.MarshalIndent(status, "", "  ")
	return mcp.NewToolResultText(string(output)), nil
}
