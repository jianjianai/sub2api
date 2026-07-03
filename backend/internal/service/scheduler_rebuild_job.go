package service

import (
	"context"
	"time"
)

type SchedulerRebuildJob struct {
	ID             int64
	Scope          string
	Reason         string
	Source         string
	SourceOutboxID *int64
	Attempts       int
}

type SchedulerRebuildJobRepository interface {
	EnqueueFullRebuild(ctx context.Context, reason string, source string, sourceOutboxID *int64) error
	ClaimNextFullRebuild(ctx context.Context, workerID string, lockTTL time.Duration) (*SchedulerRebuildJob, error)
	MarkFullRebuildSucceeded(ctx context.Context, jobID int64) error
	MarkFullRebuildFailed(ctx context.Context, jobID int64, errText string, runAfter time.Time) error
}
