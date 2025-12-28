-- Drop job schedules table, trigger, and function
DROP TRIGGER IF EXISTS update_job_schedules_updated_at ON job_schedules;
DROP FUNCTION IF EXISTS update_updated_at_column();
DROP TABLE IF EXISTS job_schedules;
