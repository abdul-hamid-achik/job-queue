-- Job execution history table
-- Records every job execution attempt for auditing and debugging
CREATE TABLE IF NOT EXISTS job_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id VARCHAR(64) NOT NULL,
    job_type VARCHAR(100) NOT NULL,
    queue VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    state VARCHAR(20) NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    max_retries INT NOT NULL DEFAULT 3,
    error_message TEXT,
    stack_trace TEXT,
    worker_id VARCHAR(100),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_ms BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX idx_job_executions_job_id ON job_executions(job_id);
CREATE INDEX idx_job_executions_job_type ON job_executions(job_type);
CREATE INDEX idx_job_executions_state ON job_executions(state);
CREATE INDEX idx_job_executions_queue ON job_executions(queue);
CREATE INDEX idx_job_executions_created_at ON job_executions(created_at);
CREATE INDEX idx_job_executions_started_at ON job_executions(started_at);

-- Composite index for filtering by type and state
CREATE INDEX idx_job_executions_type_state ON job_executions(job_type, state);

-- Comment on table
COMMENT ON TABLE job_executions IS 'Records of all job execution attempts';
COMMENT ON COLUMN job_executions.job_id IS 'The unique job identifier from the queue';
COMMENT ON COLUMN job_executions.job_type IS 'The type of job (e.g., email.send)';
COMMENT ON COLUMN job_executions.attempt IS 'Which attempt this was (1 = first try)';
COMMENT ON COLUMN job_executions.duration_ms IS 'How long the job took to execute in milliseconds';
