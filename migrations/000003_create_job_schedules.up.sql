-- Job schedules table
-- Stores recurring/cron job definitions
CREATE TABLE IF NOT EXISTS job_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) UNIQUE NOT NULL,
    job_type VARCHAR(100) NOT NULL,
    payload JSONB,
    cron_expression VARCHAR(100) NOT NULL,
    timezone VARCHAR(50) DEFAULT 'UTC',
    queue VARCHAR(50) DEFAULT 'default',
    priority VARCHAR(10) DEFAULT 'medium',
    max_retries INT DEFAULT 3,
    timeout_seconds INT DEFAULT 300,
    is_active BOOLEAN DEFAULT true,
    last_run_at TIMESTAMP WITH TIME ZONE,
    last_run_status VARCHAR(20),
    next_run_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for scheduler queries
CREATE INDEX idx_schedules_next_run ON job_schedules(next_run_at) WHERE is_active = true;
CREATE INDEX idx_schedules_is_active ON job_schedules(is_active);
CREATE INDEX idx_schedules_job_type ON job_schedules(job_type);

-- Comment on table
COMMENT ON TABLE job_schedules IS 'Cron-style recurring job definitions';
COMMENT ON COLUMN job_schedules.name IS 'Unique name for this schedule';
COMMENT ON COLUMN job_schedules.cron_expression IS 'Standard cron expression (5 or 6 fields)';
COMMENT ON COLUMN job_schedules.timezone IS 'IANA timezone for cron evaluation';
COMMENT ON COLUMN job_schedules.next_run_at IS 'Pre-calculated next run time';

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to auto-update updated_at
CREATE TRIGGER update_job_schedules_updated_at
    BEFORE UPDATE ON job_schedules
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
