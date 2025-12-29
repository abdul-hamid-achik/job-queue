package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/broker"
	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/abdul-hamid-achik/job-queue/pkg/repository"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
)

type CronScheduler struct {
	broker       broker.Broker
	scheduleRepo *repository.ScheduleRepository
	parser       cron.Parser
	pollInterval time.Duration
	logger       zerolog.Logger

	running bool
	mu      sync.RWMutex
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

type CronSchedulerOption func(*CronScheduler)

func WithCronPollInterval(d time.Duration) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.pollInterval = d
	}
}

func WithCronLogger(logger zerolog.Logger) CronSchedulerOption {
	return func(c *CronScheduler) {
		c.logger = logger
	}
}

func NewCronScheduler(b broker.Broker, repo *repository.ScheduleRepository, opts ...CronSchedulerOption) *CronScheduler {
	c := &CronScheduler{
		broker:       b,
		scheduleRepo: repo,
		parser:       cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		pollInterval: 1 * time.Second,
		logger:       zerolog.Nop(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *CronScheduler) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = true
	ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	c.logger.Info().
		Dur("poll_interval", c.pollInterval).
		Msg("cron scheduler started")

	if err := c.initializeSchedules(ctx); err != nil {
		c.logger.Error().Err(err).Msg("failed to initialize schedules")
	}

	c.wg.Add(1)
	go c.run(ctx)

	<-ctx.Done()

	c.wg.Wait()

	c.mu.Lock()
	c.running = false
	c.mu.Unlock()

	c.logger.Info().Msg("cron scheduler stopped")
	return nil
}

func (c *CronScheduler) Stop() {
	c.mu.RLock()
	cancel := c.cancel
	c.mu.RUnlock()

	if cancel != nil {
		cancel()
	}
}

func (c *CronScheduler) initializeSchedules(ctx context.Context) error {
	schedules, err := c.scheduleRepo.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("failed to list active schedules: %w", err)
	}

	for _, schedule := range schedules {
		nextRun, err := c.calculateNextRun(schedule.CronExpression, schedule.Timezone, time.Now())
		if err != nil {
			c.logger.Error().
				Err(err).
				Str("schedule_name", schedule.Name).
				Str("cron_expr", schedule.CronExpression).
				Msg("failed to calculate next run time")
			continue
		}

		if err := c.scheduleRepo.UpdateNextRun(ctx, schedule.ID, nextRun); err != nil {
			c.logger.Error().
				Err(err).
				Str("schedule_name", schedule.Name).
				Msg("failed to update next run time")
			continue
		}

		c.logger.Debug().
			Str("schedule_name", schedule.Name).
			Time("next_run", nextRun).
			Msg("initialized schedule")
	}

	return nil
}

func (c *CronScheduler) run(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.processDueSchedules(ctx); err != nil {
				c.logger.Error().Err(err).Msg("error processing due schedules")
			}
		}
	}
}

func (c *CronScheduler) processDueSchedules(ctx context.Context) error {
	now := time.Now()

	schedules, err := c.scheduleRepo.ListDue(ctx, now)
	if err != nil {
		return fmt.Errorf("failed to list due schedules: %w", err)
	}

	for _, schedule := range schedules {
		if err := c.executeSchedule(ctx, schedule, now); err != nil {
			c.logger.Error().
				Err(err).
				Str("schedule_name", schedule.Name).
				Msg("failed to execute schedule")
		}
	}

	return nil
}

func (c *CronScheduler) executeSchedule(ctx context.Context, schedule *repository.JobSchedule, now time.Time) error {
	c.logger.Info().
		Str("schedule_name", schedule.Name).
		Str("job_type", schedule.JobType).
		Msg("executing scheduled job")

	var payload json.RawMessage
	if schedule.Payload != nil {
		payload = *schedule.Payload
	} else {
		payload = json.RawMessage("{}")
	}

	j, err := job.NewWithOptions(
		schedule.JobType,
		payload,
		job.WithQueue(schedule.Queue),
		job.WithPriority(job.ParsePriority(schedule.Priority)),
		job.WithMaxRetries(schedule.MaxRetries),
		job.WithTimeout(time.Duration(schedule.TimeoutSeconds)*time.Second),
		job.WithMetadata("schedule_id", schedule.ID),
		job.WithMetadata("schedule_name", schedule.Name),
	)
	if err != nil {
		if recordErr := c.scheduleRepo.RecordRun(ctx, schedule.ID, "failed", now); recordErr != nil {
			c.logger.Error().Err(recordErr).Msg("failed to record run failure")
		}
		return fmt.Errorf("failed to create job: %w", err)
	}

	if err := c.broker.Enqueue(ctx, j); err != nil {
		if recordErr := c.scheduleRepo.RecordRun(ctx, schedule.ID, "failed", now); recordErr != nil {
			c.logger.Error().Err(recordErr).Msg("failed to record run failure")
		}
		return fmt.Errorf("failed to enqueue job: %w", err)
	}

	nextRun, err := c.calculateNextRun(schedule.CronExpression, schedule.Timezone, now)
	if err != nil {
		c.logger.Error().
			Err(err).
			Str("schedule_name", schedule.Name).
			Msg("failed to calculate next run time")
		nextRun = now.Add(1 * time.Hour)
	}

	if err := c.scheduleRepo.RecordRun(ctx, schedule.ID, "success", nextRun); err != nil {
		c.logger.Error().
			Err(err).
			Str("schedule_name", schedule.Name).
			Msg("failed to record run success")
	}

	c.logger.Info().
		Str("schedule_name", schedule.Name).
		Str("job_id", j.ID).
		Time("next_run", nextRun).
		Msg("scheduled job enqueued")

	return nil
}

func (c *CronScheduler) calculateNextRun(cronExpr, timezone string, after time.Time) (time.Time, error) {
	schedule, err := c.parser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression: %w", err)
	}

	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}

	localTime := after.In(loc)
	nextRun := schedule.Next(localTime)

	return nextRun.UTC(), nil
}

func (c *CronScheduler) IsRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running
}
