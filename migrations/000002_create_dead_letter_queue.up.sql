-- Dead letter queue table
-- Stores jobs that have exhausted all retries
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id VARCHAR(64) NOT NULL UNIQUE,
    job_type VARCHAR(100) NOT NULL,
    queue VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    priority VARCHAR(10) NOT NULL DEFAULT 'medium',
    error_message TEXT NOT NULL,
    stack_trace TEXT,
    retry_count INT NOT NULL,
    max_retries INT NOT NULL,
    original_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    failed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    requeued_at TIMESTAMP WITH TIME ZONE,
    requeue_count INT DEFAULT 0,
    metadata JSONB
);

-- Indexes for common queries
CREATE INDEX idx_dlq_job_id ON dead_letter_queue(job_id);
CREATE INDEX idx_dlq_job_type ON dead_letter_queue(job_type);
CREATE INDEX idx_dlq_queue ON dead_letter_queue(queue);
CREATE INDEX idx_dlq_failed_at ON dead_letter_queue(failed_at);
CREATE INDEX idx_dlq_requeued_at ON dead_letter_queue(requeued_at) WHERE requeued_at IS NOT NULL;

-- Comment on table
COMMENT ON TABLE dead_letter_queue IS 'Jobs that failed after all retry attempts';
COMMENT ON COLUMN dead_letter_queue.job_id IS 'Original job identifier';
COMMENT ON COLUMN dead_letter_queue.requeue_count IS 'Number of times this job has been manually requeued';
COMMENT ON COLUMN dead_letter_queue.requeued_at IS 'When the job was last requeued (NULL if never requeued)';
