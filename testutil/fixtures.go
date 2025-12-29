package testutil

import (
	"encoding/json"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/google/uuid"
)

// NewTestJob creates a test job with default values.
func NewTestJob(opts ...job.Option) *job.Job {
	j, _ := job.NewWithOptions("test.job", map[string]string{"test": "data"}, opts...)
	return j
}

// NewTestJobWithID creates a test job with a specific ID.
func NewTestJobWithID(id string, opts ...job.Option) *job.Job {
	j := NewTestJob(opts...)
	j.ID = id
	return j
}

// NewRandomTestJob creates a test job with a random ID.
func NewRandomTestJob(opts ...job.Option) *job.Job {
	return NewTestJobWithID(uuid.New().String(), opts...)
}

// NewEmailJob creates a test email job.
func NewEmailJob(to, subject string) *job.Job {
	payload := map[string]string{
		"to":      to,
		"subject": subject,
		"body":    "Test email body",
	}
	j, _ := job.NewWithOptions("email.send", payload, job.WithQueue("default"))
	return j
}

// NewFailingJob creates a job that should fail (no retries).
func NewFailingJob() *job.Job {
	return NewTestJob(job.WithMaxRetries(0))
}

// NewDelayedJob creates a job scheduled for future execution.
func NewDelayedJob(delay time.Duration) *job.Job {
	return NewTestJob(job.WithDelay(delay))
}

// NewHighPriorityJob creates a high priority test job.
func NewHighPriorityJob() *job.Job {
	return NewTestJob(job.WithPriority(job.PriorityHigh))
}

// NewLowPriorityJob creates a low priority test job.
func NewLowPriorityJob() *job.Job {
	return NewTestJob(job.WithPriority(job.PriorityLow))
}

// NewJobWithPayload creates a job with a custom payload.
func NewJobWithPayload(jobType string, payload interface{}) *job.Job {
	j, _ := job.NewWithOptions(jobType, payload)
	return j
}

// NewJobWithTimeout creates a job with a specific timeout.
func NewJobWithTimeout(timeout time.Duration) *job.Job {
	return NewTestJob(job.WithTimeout(timeout))
}

// NewJobWithRetries creates a job with a specific max retries.
func NewJobWithRetries(maxRetries int) *job.Job {
	return NewTestJob(job.WithMaxRetries(maxRetries))
}

// EmailPayload represents an email job payload.
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// ImageResizePayload represents an image resize job payload.
type ImageResizePayload struct {
	ImageURL string `json:"image_url"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
	Format   string `json:"format"`
}

// MustMarshal marshals to JSON or panics.
func MustMarshal(v interface{}) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// JobAssertions provides test assertions for jobs.
type JobAssertions struct {
	Job *job.Job
}

// Assert creates a new JobAssertions for the given job.
func Assert(j *job.Job) *JobAssertions {
	return &JobAssertions{Job: j}
}

// HasState checks if the job has the expected state.
func (a *JobAssertions) HasState(expected job.State) bool {
	return a.Job.State == expected
}

// IsCompleted checks if the job is completed.
func (a *JobAssertions) IsCompleted() bool {
	return a.HasState(job.StateCompleted)
}

// IsFailed checks if the job is failed.
func (a *JobAssertions) IsFailed() bool {
	return a.HasState(job.StateFailed)
}

// IsDead checks if the job is dead (in DLQ).
func (a *JobAssertions) IsDead() bool {
	return a.HasState(job.StateDead)
}

// HasRetryCount checks the retry count.
func (a *JobAssertions) HasRetryCount(expected int) bool {
	return a.Job.RetryCount == expected
}
