package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/abdul-hamid-achik/job-queue/internal/broker"
	"github.com/abdul-hamid-achik/job-queue/internal/job"
	"github.com/abdul-hamid-achik/job-queue/internal/repository"
	"github.com/rs/zerolog"
)

// APIHandler handles HTTP requests for the management API.
type APIHandler struct {
	broker   broker.Broker
	execRepo *repository.ExecutionRepository
	dlqRepo  *repository.DLQRepository
	logger   zerolog.Logger
}

// NewAPIHandler creates a new APIHandler.
func NewAPIHandler(
	b broker.Broker,
	execRepo *repository.ExecutionRepository,
	dlqRepo *repository.DLQRepository,
	logger zerolog.Logger,
) *APIHandler {
	return &APIHandler{
		broker:   b,
		execRepo: execRepo,
		dlqRepo:  dlqRepo,
		logger:   logger,
	}
}

// RegisterRoutes registers all API routes.
func (h *APIHandler) RegisterRoutes(mux *http.ServeMux) {
	// Health check
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("GET /ready", h.Ready)

	// Jobs
	mux.HandleFunc("POST /api/v1/jobs", h.CreateJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}", h.GetJob)
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", h.DeleteJob)

	// Queues
	mux.HandleFunc("GET /api/v1/queues", h.ListQueues)
	mux.HandleFunc("GET /api/v1/queues/{name}/depth", h.GetQueueDepth)

	// Dead Letter Queue
	mux.HandleFunc("GET /api/v1/dlq", h.ListDLQ)
	mux.HandleFunc("GET /api/v1/dlq/{id}", h.GetDLQJob)
	mux.HandleFunc("POST /api/v1/dlq/{id}/retry", h.RetryDLQJob)
	mux.HandleFunc("DELETE /api/v1/dlq/{id}", h.DeleteDLQJob)

	// Job Executions
	mux.HandleFunc("GET /api/v1/executions", h.ListExecutions)
	mux.HandleFunc("GET /api/v1/executions/{jobID}", h.GetExecutions)

	// Stats
	mux.HandleFunc("GET /api/v1/stats", h.GetStats)
}

// Health returns a simple health check response.
func (h *APIHandler) Health(w http.ResponseWriter, r *http.Request) {
	h.jsonResponse(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready checks if all dependencies are ready.
func (h *APIHandler) Ready(w http.ResponseWriter, r *http.Request) {
	if err := h.broker.Ping(r.Context()); err != nil {
		h.errorResponse(w, http.StatusServiceUnavailable, "redis not ready")
		return
	}
	h.jsonResponse(w, http.StatusOK, map[string]string{"status": "ready"})
}

// CreateJobRequest is the request body for creating a job.
type CreateJobRequest struct {
	Type        string            `json:"type"`
	Payload     json.RawMessage   `json:"payload"`
	Queue       string            `json:"queue,omitempty"`
	Priority    string            `json:"priority,omitempty"`
	MaxRetries  int               `json:"max_retries,omitempty"`
	Timeout     string            `json:"timeout,omitempty"`
	ScheduledAt *time.Time        `json:"scheduled_at,omitempty"`
	Delay       string            `json:"delay,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// CreateJob creates and enqueues a new job.
func (h *APIHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Type == "" {
		h.errorResponse(w, http.StatusBadRequest, "job type is required")
		return
	}

	// Build job options
	opts := []job.Option{}

	if req.Queue != "" {
		opts = append(opts, job.WithQueue(req.Queue))
	}
	if req.Priority != "" {
		opts = append(opts, job.WithPriority(job.ParsePriority(req.Priority)))
	}
	if req.MaxRetries > 0 {
		opts = append(opts, job.WithMaxRetries(req.MaxRetries))
	}
	if req.Timeout != "" {
		if d, err := time.ParseDuration(req.Timeout); err == nil {
			opts = append(opts, job.WithTimeout(d))
		}
	}
	if req.ScheduledAt != nil {
		opts = append(opts, job.WithScheduledAt(*req.ScheduledAt))
	}
	if req.Delay != "" {
		if d, err := time.ParseDuration(req.Delay); err == nil {
			opts = append(opts, job.WithDelay(d))
		}
	}
	for k, v := range req.Metadata {
		opts = append(opts, job.WithMetadata(k, v))
	}

	// Create job
	j, err := job.NewWithOptions(req.Type, req.Payload, opts...)
	if err != nil {
		h.errorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	// Enqueue
	if err := h.broker.Enqueue(r.Context(), j); err != nil {
		h.logger.Error().Err(err).Str("job_id", j.ID).Msg("failed to enqueue job")
		h.errorResponse(w, http.StatusInternalServerError, "failed to enqueue job")
		return
	}

	h.logger.Info().Str("job_id", j.ID).Str("type", j.Type).Msg("job created")
	h.jsonResponse(w, http.StatusCreated, j)
}

// GetJob retrieves a job by ID.
func (h *APIHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		h.errorResponse(w, http.StatusBadRequest, "job ID is required")
		return
	}

	j, err := h.broker.GetJob(r.Context(), id)
	if err == broker.ErrJobNotFound {
		h.errorResponse(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to get job")
		return
	}

	h.jsonResponse(w, http.StatusOK, j)
}

// DeleteJob cancels/deletes a job.
func (h *APIHandler) DeleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		h.errorResponse(w, http.StatusBadRequest, "job ID is required")
		return
	}

	if err := h.broker.DeleteJob(r.Context(), id); err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to delete job")
		return
	}

	h.jsonResponse(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// ListQueues returns all queues with their depths.
func (h *APIHandler) ListQueues(w http.ResponseWriter, r *http.Request) {
	// For now, return common queues
	queues := []string{"default", "critical", "low"}
	result := make([]map[string]interface{}, 0, len(queues))

	for _, queue := range queues {
		depth, _ := h.broker.GetQueueDepth(r.Context(), queue)
		result = append(result, map[string]interface{}{
			"name":  queue,
			"depth": depth,
		})
	}

	h.jsonResponse(w, http.StatusOK, result)
}

// GetQueueDepth returns the depth of a specific queue.
func (h *APIHandler) GetQueueDepth(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		h.errorResponse(w, http.StatusBadRequest, "queue name is required")
		return
	}

	depth, err := h.broker.GetQueueDepth(r.Context(), name)
	if err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to get queue depth")
		return
	}

	h.jsonResponse(w, http.StatusOK, map[string]interface{}{
		"queue": name,
		"depth": depth,
	})
}

// ListDLQ returns jobs in the dead letter queue.
func (h *APIHandler) ListDLQ(w http.ResponseWriter, r *http.Request) {
	filter := repository.DLQFilter{
		Limit: 100,
	}

	if l := r.URL.Query().Get("limit"); l != "" {
		if limit, err := strconv.Atoi(l); err == nil {
			filter.Limit = limit
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if offset, err := strconv.Atoi(o); err == nil {
			filter.Offset = offset
		}
	}
	if t := r.URL.Query().Get("type"); t != "" {
		filter.JobType = &t
	}
	if q := r.URL.Query().Get("queue"); q != "" {
		filter.Queue = &q
	}

	jobs, err := h.dlqRepo.List(r.Context(), filter)
	if err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to list DLQ")
		return
	}

	h.jsonResponse(w, http.StatusOK, jobs)
}

// GetDLQJob retrieves a specific DLQ job.
func (h *APIHandler) GetDLQJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		h.errorResponse(w, http.StatusBadRequest, "ID is required")
		return
	}

	dlj, err := h.dlqRepo.GetByID(r.Context(), id)
	if err == repository.ErrNotFound {
		h.errorResponse(w, http.StatusNotFound, "job not found in DLQ")
		return
	}
	if err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to get DLQ job")
		return
	}

	h.jsonResponse(w, http.StatusOK, dlj)
}

// RetryDLQJob moves a job from DLQ back to the queue.
func (h *APIHandler) RetryDLQJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		h.errorResponse(w, http.StatusBadRequest, "ID is required")
		return
	}

	// Get the DLQ job
	dlj, err := h.dlqRepo.GetByID(r.Context(), id)
	if err == repository.ErrNotFound {
		h.errorResponse(w, http.StatusNotFound, "job not found in DLQ")
		return
	}
	if err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to get DLQ job")
		return
	}

	// Convert back to a job
	j := dlj.ToJob()

	// Re-enqueue
	if err := h.broker.Enqueue(r.Context(), j); err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to requeue job")
		return
	}

	// Mark as requeued
	if err := h.dlqRepo.MarkRequeued(r.Context(), id); err != nil {
		h.logger.Error().Err(err).Msg("failed to mark DLQ job as requeued")
	}

	h.logger.Info().Str("job_id", j.ID).Msg("job requeued from DLQ")
	h.jsonResponse(w, http.StatusOK, j)
}

// DeleteDLQJob removes a job from the DLQ.
func (h *APIHandler) DeleteDLQJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		h.errorResponse(w, http.StatusBadRequest, "ID is required")
		return
	}

	if err := h.dlqRepo.Delete(r.Context(), id); err == repository.ErrNotFound {
		h.errorResponse(w, http.StatusNotFound, "job not found in DLQ")
		return
	} else if err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to delete DLQ job")
		return
	}

	h.jsonResponse(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// ListExecutions returns job execution history.
func (h *APIHandler) ListExecutions(w http.ResponseWriter, r *http.Request) {
	filter := repository.ExecutionFilter{
		Limit: 100,
	}

	if l := r.URL.Query().Get("limit"); l != "" {
		if limit, err := strconv.Atoi(l); err == nil {
			filter.Limit = limit
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if offset, err := strconv.Atoi(o); err == nil {
			filter.Offset = offset
		}
	}
	if t := r.URL.Query().Get("type"); t != "" {
		filter.JobType = &t
	}
	if s := r.URL.Query().Get("state"); s != "" {
		filter.State = &s
	}

	executions, err := h.execRepo.List(r.Context(), filter)
	if err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to list executions")
		return
	}

	h.jsonResponse(w, http.StatusOK, executions)
}

// GetExecutions returns execution history for a specific job.
func (h *APIHandler) GetExecutions(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	if jobID == "" {
		h.errorResponse(w, http.StatusBadRequest, "job ID is required")
		return
	}

	executions, err := h.execRepo.GetByJobID(r.Context(), jobID)
	if err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to get executions")
		return
	}

	h.jsonResponse(w, http.StatusOK, executions)
}

// GetStats returns overall statistics.
func (h *APIHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	fromDate := now.Add(-24 * time.Hour)

	stats, err := h.execRepo.GetStats(r.Context(), fromDate, now)
	if err != nil {
		h.errorResponse(w, http.StatusInternalServerError, "failed to get stats")
		return
	}

	dlqCount, _ := h.dlqRepo.Count(r.Context())

	h.jsonResponse(w, http.StatusOK, map[string]interface{}{
		"executions_24h":    stats,
		"dead_letter_count": dlqCount,
	})
}

func (h *APIHandler) jsonResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *APIHandler) errorResponse(w http.ResponseWriter, status int, message string) {
	h.jsonResponse(w, status, map[string]string{"error": message})
}
