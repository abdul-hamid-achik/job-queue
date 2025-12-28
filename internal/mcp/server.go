package mcp

import (
	"github.com/abdul-hamid-achik/job-queue/internal/broker"
	"github.com/abdul-hamid-achik/job-queue/internal/repository"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// Server wraps the MCP server with job queue functionality.
type Server struct {
	mcpServer *server.MCPServer
	broker    broker.Broker
	execRepo  *repository.ExecutionRepository
	dlqRepo   *repository.DLQRepository
}

// NewServer creates a new MCP server for the job queue.
func NewServer(b broker.Broker, execRepo *repository.ExecutionRepository, dlqRepo *repository.DLQRepository) *Server {
	s := &Server{
		broker:   b,
		execRepo: execRepo,
		dlqRepo:  dlqRepo,
	}

	s.mcpServer = server.NewMCPServer(
		"job-queue",
		"0.1.0",
		server.WithToolCapabilities(true),
	)

	s.registerTools()

	return s
}

// registerTools registers all MCP tools.
func (s *Server) registerTools() {
	s.mcpServer.AddTool(mcp.NewTool("enqueue_job",
		mcp.WithDescription("Create and enqueue a new background job"),
		mcp.WithString("type",
			mcp.Required(),
			mcp.Description("Job type identifier (e.g., 'email.send', 'payment.process')"),
		),
		mcp.WithObject("payload",
			mcp.Description("Job payload data as JSON object"),
		),
		mcp.WithString("queue",
			mcp.Description("Queue name (default: 'default')"),
		),
		mcp.WithString("priority",
			mcp.Description("Job priority: critical, high, medium, low (default: medium)"),
		),
		mcp.WithNumber("max_retries",
			mcp.Description("Maximum retry attempts (default: 3)"),
		),
		mcp.WithString("delay",
			mcp.Description("Delay before execution (e.g., '1h', '30m', '5s')"),
		),
	), s.handleEnqueueJob)

	s.mcpServer.AddTool(mcp.NewTool("get_job",
		mcp.WithDescription("Get job details by ID"),
		mcp.WithString("id",
			mcp.Required(),
			mcp.Description("Job ID (UUID)"),
		),
	), s.handleGetJob)

	s.mcpServer.AddTool(mcp.NewTool("delete_job",
		mcp.WithDescription("Delete/cancel a job by ID"),
		mcp.WithString("id",
			mcp.Required(),
			mcp.Description("Job ID (UUID)"),
		),
	), s.handleDeleteJob)

	s.mcpServer.AddTool(mcp.NewTool("list_queues",
		mcp.WithDescription("List all queues with their current depths"),
	), s.handleListQueues)

	s.mcpServer.AddTool(mcp.NewTool("get_queue_depth",
		mcp.WithDescription("Get the number of jobs in a specific queue"),
		mcp.WithString("name",
			mcp.Required(),
			mcp.Description("Queue name"),
		),
	), s.handleGetQueueDepth)

	s.mcpServer.AddTool(mcp.NewTool("list_dlq",
		mcp.WithDescription("List jobs in the dead letter queue"),
		mcp.WithNumber("limit",
			mcp.Description("Maximum number of jobs to return (default: 100)"),
		),
		mcp.WithNumber("offset",
			mcp.Description("Number of jobs to skip (default: 0)"),
		),
		mcp.WithString("type",
			mcp.Description("Filter by job type"),
		),
		mcp.WithString("queue",
			mcp.Description("Filter by queue"),
		),
	), s.handleListDLQ)

	s.mcpServer.AddTool(mcp.NewTool("get_dlq_job",
		mcp.WithDescription("Get a specific job from the dead letter queue"),
		mcp.WithString("id",
			mcp.Required(),
			mcp.Description("DLQ record ID"),
		),
	), s.handleGetDLQJob)

	s.mcpServer.AddTool(mcp.NewTool("retry_dlq_job",
		mcp.WithDescription("Retry a job from the dead letter queue"),
		mcp.WithString("id",
			mcp.Required(),
			mcp.Description("DLQ record ID"),
		),
	), s.handleRetryDLQJob)

	s.mcpServer.AddTool(mcp.NewTool("delete_dlq_job",
		mcp.WithDescription("Permanently remove a job from the dead letter queue"),
		mcp.WithString("id",
			mcp.Required(),
			mcp.Description("DLQ record ID"),
		),
	), s.handleDeleteDLQJob)

	s.mcpServer.AddTool(mcp.NewTool("list_executions",
		mcp.WithDescription("List job execution history"),
		mcp.WithNumber("limit",
			mcp.Description("Maximum number of executions to return (default: 100)"),
		),
		mcp.WithNumber("offset",
			mcp.Description("Number of executions to skip (default: 0)"),
		),
		mcp.WithString("type",
			mcp.Description("Filter by job type"),
		),
		mcp.WithString("state",
			mcp.Description("Filter by state: pending, queued, processing, completed, failed, dead"),
		),
	), s.handleListExecutions)

	s.mcpServer.AddTool(mcp.NewTool("get_job_executions",
		mcp.WithDescription("Get execution history for a specific job"),
		mcp.WithString("job_id",
			mcp.Required(),
			mcp.Description("Job ID (UUID)"),
		),
	), s.handleGetJobExecutions)

	s.mcpServer.AddTool(mcp.NewTool("get_stats",
		mcp.WithDescription("Get overall job execution statistics for the last 24 hours"),
	), s.handleGetStats)

	s.mcpServer.AddTool(mcp.NewTool("health_check",
		mcp.WithDescription("Check if the job queue system is healthy and all dependencies are ready"),
	), s.handleHealthCheck)
}

// ServeStdio starts the MCP server using stdio transport.
func (s *Server) ServeStdio() error {
	return server.ServeStdio(s.mcpServer)
}
