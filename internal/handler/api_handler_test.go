package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/abdul-hamid-achik/job-queue/internal/job"
	"github.com/abdul-hamid-achik/job-queue/testutil"
	"github.com/rs/zerolog"
)

func newTestHandler() *APIHandler {
	mb := testutil.NewMockBroker()
	logger := zerolog.Nop()
	return NewAPIHandler(mb, nil, nil, logger)
}

func TestAPIHandler_Health(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	h.Health(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("expected status 'ok', got '%s'", resp["status"])
	}
}

func TestAPIHandler_Ready(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	h.Ready(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["status"] != "ready" {
		t.Errorf("expected status 'ready', got '%s'", resp["status"])
	}
}

func TestAPIHandler_CreateJob(t *testing.T) {
	h := newTestHandler()

	body := `{"type": "email.send", "payload": {"to": "test@example.com"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected status %d, got %d. Body: %s", http.StatusCreated, w.Code, w.Body.String())
	}

	var resp job.Job
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.ID == "" {
		t.Error("expected job ID to be set")
	}
	if resp.Type != "email.send" {
		t.Errorf("expected job type 'email.send', got '%s'", resp.Type)
	}
}

func TestAPIHandler_CreateJob_InvalidBody(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestAPIHandler_CreateJob_MissingType(t *testing.T) {
	h := newTestHandler()

	body := `{"payload": {"to": "test@example.com"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["error"] != "job type is required" {
		t.Errorf("expected error 'job type is required', got '%s'", resp["error"])
	}
}

func TestAPIHandler_CreateJob_WithOptions(t *testing.T) {
	h := newTestHandler()

	body := `{
		"type": "email.send",
		"payload": {"to": "test@example.com"},
		"queue": "critical",
		"priority": "high",
		"max_retries": 5,
		"timeout": "30s"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected status %d, got %d. Body: %s", http.StatusCreated, w.Code, w.Body.String())
	}

	var resp job.Job
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Queue != "critical" {
		t.Errorf("expected queue 'critical', got '%s'", resp.Queue)
	}
	if resp.Priority != job.PriorityHigh {
		t.Errorf("expected priority 'high', got '%s'", resp.Priority)
	}
	if resp.MaxRetries != 5 {
		t.Errorf("expected max_retries 5, got %d", resp.MaxRetries)
	}
}

func TestAPIHandler_GetJob(t *testing.T) {
	mb := testutil.NewMockBroker()
	logger := zerolog.Nop()
	h := NewAPIHandler(mb, nil, nil, logger)

	// Create a job first
	j := testutil.NewTestJob()
	mb.Enqueue(nil, j)

	// Create request with path value
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+j.ID, nil)
	req.SetPathValue("id", j.ID)
	w := httptest.NewRecorder()

	h.GetJob(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d. Body: %s", http.StatusOK, w.Code, w.Body.String())
	}

	var resp job.Job
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.ID != j.ID {
		t.Errorf("expected job ID '%s', got '%s'", j.ID, resp.ID)
	}
}

func TestAPIHandler_GetJob_NotFound(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/nonexistent", nil)
	req.SetPathValue("id", "nonexistent")
	w := httptest.NewRecorder()

	h.GetJob(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestAPIHandler_GetJob_MissingID(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/", nil)
	req.SetPathValue("id", "")
	w := httptest.NewRecorder()

	h.GetJob(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestAPIHandler_DeleteJob(t *testing.T) {
	mb := testutil.NewMockBroker()
	logger := zerolog.Nop()
	h := NewAPIHandler(mb, nil, nil, logger)

	// Create a job first
	j := testutil.NewTestJob()
	mb.Enqueue(nil, j)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/"+j.ID, nil)
	req.SetPathValue("id", j.ID)
	w := httptest.NewRecorder()

	h.DeleteJob(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["status"] != "deleted" {
		t.Errorf("expected status 'deleted', got '%s'", resp["status"])
	}
}

func TestAPIHandler_ListQueues(t *testing.T) {
	h := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/queues", nil)
	w := httptest.NewRecorder()

	h.ListQueues(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp []map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp) != 3 {
		t.Errorf("expected 3 queues, got %d", len(resp))
	}

	// Check expected queues exist
	expectedQueues := map[string]bool{"default": false, "critical": false, "low": false}
	for _, q := range resp {
		name, ok := q["name"].(string)
		if !ok {
			continue
		}
		if _, exists := expectedQueues[name]; exists {
			expectedQueues[name] = true
		}
	}

	for name, found := range expectedQueues {
		if !found {
			t.Errorf("expected queue '%s' not found", name)
		}
	}
}

func TestAPIHandler_GetQueueDepth(t *testing.T) {
	mb := testutil.NewMockBroker()
	logger := zerolog.Nop()
	h := NewAPIHandler(mb, nil, nil, logger)

	// Enqueue some jobs
	for i := 0; i < 3; i++ {
		mb.Enqueue(nil, testutil.NewTestJob())
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/queues/default/depth", nil)
	req.SetPathValue("name", "default")
	w := httptest.NewRecorder()

	h.GetQueueDepth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["queue"] != "default" {
		t.Errorf("expected queue 'default', got '%v'", resp["queue"])
	}

	depth, ok := resp["depth"].(float64)
	if !ok {
		t.Fatal("expected depth to be a number")
	}
	if int(depth) != 3 {
		t.Errorf("expected depth 3, got %v", depth)
	}
}

func TestAPIHandler_RegisterRoutes(t *testing.T) {
	h := newTestHandler()
	mux := http.NewServeMux()

	h.RegisterRoutes(mux)

	// Test routes that don't require database repositories
	tests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/health"},
		{http.MethodGet, "/ready"},
		{http.MethodGet, "/api/v1/queues"},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(tt.method, tt.path, nil)
		w := httptest.NewRecorder()

		mux.ServeHTTP(w, req)

		// Should not return 404 (route not found)
		if w.Code == http.StatusNotFound {
			t.Errorf("route %s %s not registered", tt.method, tt.path)
		}
	}
}
