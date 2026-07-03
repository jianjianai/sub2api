package repository

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestSchedulerRebuildJobEnqueueCoalescesActiveFullJob(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	repo := &schedulerRebuildJobRepository{db: db}
	selectActive := `(?s)SELECT id.*FROM scheduler_rebuild_jobs.*status IN \('pending', 'running'\).*FOR UPDATE`

	mock.ExpectBegin()
	mock.ExpectQuery(selectActive).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`(?s)INSERT INTO scheduler_rebuild_jobs \(scope, reason, source, source_outbox_id\).*VALUES \('full', \$1, \$2, \$3\)`).
		WithArgs("startup", "startup", nil).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectQuery(selectActive).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
	mock.ExpectExec(`(?s)UPDATE scheduler_rebuild_jobs.*SET reason = \$1,.*source = \$2,.*source_outbox_id = COALESCE\(\$3, source_outbox_id\).*WHERE id = \$4`).
		WithArgs("interval", "interval", nil, int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	require.NoError(t, repo.EnqueueFullRebuild(context.Background(), "startup", "startup", nil))
	require.NoError(t, repo.EnqueueFullRebuild(context.Background(), "interval", "interval", nil))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSchedulerRebuildJobClaimUsesSkipLocked(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	repo := &schedulerRebuildJobRepository{db: db}
	mock.ExpectQuery(`(?s)FOR UPDATE SKIP LOCKED.*UPDATE scheduler_rebuild_jobs j`).
		WithArgs("worker-1", sqlmock.AnyArg()).
		WillReturnError(sql.ErrNoRows)

	job, err := repo.ClaimNextFullRebuild(context.Background(), "worker-1", time.Minute)
	require.NoError(t, err)
	require.Nil(t, job)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSchedulerRebuildJobFailedReturnsToPending(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	repo := &schedulerRebuildJobRepository{db: db}
	runAfter := time.Now().Add(time.Minute)
	mock.ExpectExec(`(?s)UPDATE scheduler_rebuild_jobs.*SET status = 'pending',.*run_after = \$2,.*last_error = \$3,.*WHERE id = \$1`).
		WithArgs(int64(9), runAfter, "boom").
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, repo.MarkFullRebuildFailed(context.Background(), 9, "boom", runAfter))
	require.True(t, runAfter.After(time.Now()))
	require.NoError(t, mock.ExpectationsWereMet())
}
