package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type schedulerOutboxRepository struct {
	db *sql.DB
}

type schedulerOutboxCleanupLease struct {
	conn *sql.Conn
}

const (
	schedulerOutboxDedupWindow      = time.Second
	schedulerOutboxDefaultCleanSize = 5000
)

func NewSchedulerOutboxRepository(db *sql.DB) service.SchedulerOutboxRepository {
	return &schedulerOutboxRepository{db: db}
}

func (r *schedulerOutboxRepository) ListAfter(ctx context.Context, afterID int64, limit int) ([]service.SchedulerOutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, event_type, account_id, group_id, payload, created_at
		FROM scheduler_outbox
		WHERE id > $1
		ORDER BY id ASC
		LIMIT $2
	`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	events := make([]service.SchedulerOutboxEvent, 0, limit)
	for rows.Next() {
		var (
			payloadRaw []byte
			accountID  sql.NullInt64
			groupID    sql.NullInt64
			event      service.SchedulerOutboxEvent
		)
		if err := rows.Scan(&event.ID, &event.EventType, &accountID, &groupID, &payloadRaw, &event.CreatedAt); err != nil {
			return nil, err
		}
		if accountID.Valid {
			v := accountID.Int64
			event.AccountID = &v
		}
		if groupID.Valid {
			v := groupID.Int64
			event.GroupID = &v
		}
		if len(payloadRaw) > 0 {
			var payload map[string]any
			if err := json.Unmarshal(payloadRaw, &payload); err != nil {
				return nil, err
			}
			event.Payload = payload
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (r *schedulerOutboxRepository) MaxID(ctx context.Context) (int64, error) {
	var maxID int64
	if err := r.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(id), 0) FROM scheduler_outbox").Scan(&maxID); err != nil {
		return 0, err
	}
	return maxID, nil
}

func (r *schedulerOutboxRepository) DeleteConsumedUpTo(ctx context.Context, watermark int64, limit int) (int64, error) {
	if watermark <= 0 {
		return 0, nil
	}
	if limit <= 0 {
		limit = schedulerOutboxDefaultCleanSize
	}
	result, err := r.db.ExecContext(ctx, `
		WITH doomed AS (
			SELECT id
			FROM scheduler_outbox
			WHERE id <= $1
			ORDER BY id ASC
			LIMIT $2
		)
		DELETE FROM scheduler_outbox o
		USING doomed d
		WHERE o.id = d.id
	`, watermark, limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *schedulerOutboxRepository) TryAcquireCleanupLock(ctx context.Context) (service.SchedulerOutboxCleanupLease, bool, error) {
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}

	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(hashtext('scheduler_outbox_cleanup'))").Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	if !acquired {
		_ = conn.Close()
		return nil, false, nil
	}
	return &schedulerOutboxCleanupLease{conn: conn}, true, nil
}

func (l *schedulerOutboxCleanupLease) Release() {
	if l == nil || l.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = l.conn.ExecContext(ctx, "SELECT pg_advisory_unlock(hashtext('scheduler_outbox_cleanup'))")
	_ = l.conn.Close()
	l.conn = nil
}

func enqueueSchedulerOutbox(ctx context.Context, exec sqlExecutor, eventType string, accountID *int64, groupID *int64, payload any) error {
	if exec == nil {
		return nil
	}
	var payloadArg any
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		payloadArg = encoded
	}
	query := `
		INSERT INTO scheduler_outbox (event_type, account_id, group_id, payload)
		VALUES ($1, $2, $3, $4)
	`
	args := []any{eventType, accountID, groupID, payloadArg}
	if schedulerOutboxEventSupportsDedup(eventType) {
		query = `
			INSERT INTO scheduler_outbox (event_type, account_id, group_id, payload)
			SELECT $1, $2, $3, $4
			WHERE NOT EXISTS (
				SELECT 1
				FROM scheduler_outbox
				WHERE event_type = $1
					AND account_id IS NOT DISTINCT FROM $2
					AND group_id IS NOT DISTINCT FROM $3
					AND created_at >= NOW() - make_interval(secs => $5)
			)
		`
		args = append(args, schedulerOutboxDedupWindow.Seconds())
	}
	_, err := exec.ExecContext(ctx, query, args...)
	return err
}

func schedulerOutboxEventSupportsDedup(eventType string) bool {
	switch eventType {
	case service.SchedulerOutboxEventAccountChanged,
		service.SchedulerOutboxEventGroupChanged,
		service.SchedulerOutboxEventFullRebuild:
		return true
	default:
		return false
	}
}
