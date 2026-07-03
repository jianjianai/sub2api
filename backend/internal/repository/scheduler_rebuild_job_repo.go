package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

type schedulerRebuildJobRepository struct {
	db *sql.DB
}

func NewSchedulerRebuildJobRepository(db *sql.DB) service.SchedulerRebuildJobRepository {
	return &schedulerRebuildJobRepository{db: db}
}

func (r *schedulerRebuildJobRepository) EnqueueFullRebuild(ctx context.Context, reason string, source string, sourceOutboxID *int64) error {
	for attempt := 0; attempt < 2; attempt++ {
		retry, err := r.enqueueFullRebuildTx(ctx, reason, source, sourceOutboxID)
		if err == nil {
			return nil
		}
		if retry && attempt == 0 {
			continue
		}
		return err
	}
	return nil
}

func (r *schedulerRebuildJobRepository) enqueueFullRebuildTx(ctx context.Context, reason string, source string, sourceOutboxID *int64) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("scheduler rebuild job repository is not configured")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var existingID int64
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM scheduler_rebuild_jobs
		WHERE scope = 'full'
		  AND status IN ('pending', 'running')
		ORDER BY id ASC
		LIMIT 1
		FOR UPDATE
	`).Scan(&existingID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}

	var sourceOutboxArg any
	if sourceOutboxID != nil {
		sourceOutboxArg = *sourceOutboxID
	}
	if err == nil {
		if _, err := tx.ExecContext(ctx, `
			UPDATE scheduler_rebuild_jobs
			SET reason = $1,
				source = $2,
				source_outbox_id = COALESCE($3, source_outbox_id),
				run_after = LEAST(run_after, NOW()),
				updated_at = NOW()
			WHERE id = $4
		`, reason, source, sourceOutboxArg, existingID); err != nil {
			return false, err
		}
		return false, tx.Commit()
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO scheduler_rebuild_jobs (scope, reason, source, source_outbox_id)
		VALUES ('full', $1, $2, $3)
	`, reason, source, sourceOutboxArg); err != nil {
		return isSchedulerRebuildJobUniqueViolation(err), err
	}
	return false, tx.Commit()
}

func (r *schedulerRebuildJobRepository) ClaimNextFullRebuild(ctx context.Context, workerID string, lockTTL time.Duration) (*service.SchedulerRebuildJob, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("scheduler rebuild job repository is not configured")
	}
	lockedUntil := time.Now().UTC().Add(lockTTL)
	row := r.db.QueryRowContext(ctx, `
		WITH candidate AS (
			SELECT id
			FROM scheduler_rebuild_jobs
			WHERE scope = 'full'
			  AND (
				  status = 'pending'
				  OR (status = 'running' AND locked_until < NOW())
			  )
			  AND run_after <= NOW()
			ORDER BY id ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE scheduler_rebuild_jobs j
		SET status = 'running',
			locked_by = $1,
			locked_until = $2,
			attempts = attempts + 1,
			updated_at = NOW()
		FROM candidate
		WHERE j.id = candidate.id
		RETURNING j.id, j.scope, j.reason, j.source, j.source_outbox_id, j.attempts
	`, workerID, lockedUntil)

	var (
		job            service.SchedulerRebuildJob
		sourceOutboxID sql.NullInt64
	)
	if err := row.Scan(&job.ID, &job.Scope, &job.Reason, &job.Source, &sourceOutboxID, &job.Attempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if sourceOutboxID.Valid {
		v := sourceOutboxID.Int64
		job.SourceOutboxID = &v
	}
	return &job, nil
}

func (r *schedulerRebuildJobRepository) MarkFullRebuildSucceeded(ctx context.Context, jobID int64) error {
	if r == nil || r.db == nil || jobID <= 0 {
		return nil
	}
	_, err := r.db.ExecContext(ctx, `
		UPDATE scheduler_rebuild_jobs
		SET status = 'succeeded',
			locked_by = NULL,
			locked_until = NULL,
			last_error = NULL,
			updated_at = NOW(),
			completed_at = NOW()
		WHERE id = $1
	`, jobID)
	return err
}

func (r *schedulerRebuildJobRepository) MarkFullRebuildFailed(ctx context.Context, jobID int64, errText string, runAfter time.Time) error {
	if r == nil || r.db == nil || jobID <= 0 {
		return nil
	}
	_, err := r.db.ExecContext(ctx, `
		UPDATE scheduler_rebuild_jobs
		SET status = 'pending',
			run_after = $2,
			locked_by = NULL,
			locked_until = NULL,
			last_error = $3,
			updated_at = NOW()
		WHERE id = $1
	`, jobID, runAfter, errText)
	return err
}

func isSchedulerRebuildJobUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && string(pqErr.Code) == "23505"
}
