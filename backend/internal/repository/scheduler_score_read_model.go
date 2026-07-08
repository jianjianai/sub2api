package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const (
	schedulerScoreRedisChunkSize      = 1000
	schedulerScoreInactiveVersionTTL  = 10 * time.Minute
	schedulerScoreOldActiveVersionTTL = 5 * time.Minute
	schedulerScoreUpdatedAtTimeFormat = time.RFC3339Nano
	schedulerScoreRedisKeyPrefix      = "scheduler_score:v1"
	schedulerScoreMetaFieldVersion    = "version"
	schedulerScoreMetaFieldUpdatedAt  = "updated_at"
	schedulerScoreMetaFieldBucketSize = "bucket_size"
)

var schedulerScoreActivateScript = redis.NewScript(`
local active = redis.call("GET", KEYS[1])
local new_version = tonumber(ARGV[1])
if active ~= false and tonumber(active) ~= nil and new_version < tonumber(active) then
	return 0
end
redis.call("PERSIST", KEYS[2])
redis.call("PERSIST", KEYS[3])
redis.call("SET", KEYS[1], ARGV[1])
if ARGV[3] == "1" then
	redis.call("EXPIRE", KEYS[4], ARGV[2])
	redis.call("EXPIRE", KEYS[5], ARGV[2])
end
return 1
`)

type schedulerScoreReadModel struct {
	rdb *redis.Client
}

type noopSchedulerScoreReadModel struct{}

func NewSchedulerScoreReadModel(rdb *redis.Client) service.SchedulerScoreReadModel {
	if rdb == nil {
		return &noopSchedulerScoreReadModel{}
	}
	return &schedulerScoreReadModel{rdb: rdb}
}

func (m *schedulerScoreReadModel) NextVersion(ctx context.Context, bucket service.SchedulerBucket) (int64, error) {
	if m == nil || m.rdb == nil {
		return 0, nil
	}
	return m.rdb.Incr(ctx, schedulerScoreVersionKey(bucket)).Result()
}

func (m *schedulerScoreReadModel) GetScores(ctx context.Context, bucket service.SchedulerBucket, accountIDs []int64) (map[int64]service.SchedulerScoreSnapshot, error) {
	result := make(map[int64]service.SchedulerScoreSnapshot)
	if m == nil || m.rdb == nil || len(accountIDs) == 0 {
		return result, nil
	}
	fields := schedulerScoreAccountIDFields(accountIDs)
	if len(fields) == 0 {
		return result, nil
	}
	active, err := m.rdb.Get(ctx, schedulerScoreActiveKey(bucket)).Result()
	if errors.Is(err, redis.Nil) || active == "" {
		return result, nil
	}
	if err != nil {
		return nil, err
	}

	values, err := m.rdb.HMGet(ctx, schedulerScoreScoresKey(bucket, active), fields...).Result()
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		if value == nil {
			continue
		}
		raw, ok := value.(string)
		if !ok || raw == "" {
			continue
		}
		var snapshot service.SchedulerScoreSnapshot
		if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
			slog.Warn("scheduler_score_snapshot_decode_failed", "bucket", bucket.String(), "account_id", fields[i], "error", err)
			continue
		}
		result[snapshot.AccountID] = snapshot
	}
	return result, nil
}

func (m *schedulerScoreReadModel) SetBucketScores(ctx context.Context, bucket service.SchedulerBucket, scores []service.SchedulerScoreSnapshot, version int64, updatedAt time.Time) error {
	if m == nil || m.rdb == nil {
		return nil
	}
	if version <= 0 {
		return fmt.Errorf("scheduler score version must be positive")
	}

	versionText := strconv.FormatInt(version, 10)
	scoresKey := schedulerScoreScoresKey(bucket, versionText)
	metaKey := schedulerScoreMetaKey(bucket, versionText)

	// 先写未激活版本并给临时 TTL，避免进程在 active 切换前崩溃时留下永不过期的孤儿版本。
	for start := 0; start < len(scores); start += schedulerScoreRedisChunkSize {
		end := start + schedulerScoreRedisChunkSize
		if end > len(scores) {
			end = len(scores)
		}
		args := make([]any, 0, (end-start)*2)
		for _, score := range scores[start:end] {
			payload, err := json.Marshal(score)
			if err != nil {
				return fmt.Errorf("marshal scheduler score: %w", err)
			}
			args = append(args, strconv.FormatInt(score.AccountID, 10), payload)
		}
		if len(args) > 0 {
			if err := m.rdb.HSet(ctx, scoresKey, args...).Err(); err != nil {
				return err
			}
		}
	}
	bucketSize := len(scores)
	if len(scores) > 0 {
		bucketSize = scores[0].BucketSize
	}
	if err := m.rdb.HSet(ctx, metaKey,
		schedulerScoreMetaFieldVersion, versionText,
		schedulerScoreMetaFieldUpdatedAt, updatedAt.UTC().Format(schedulerScoreUpdatedAtTimeFormat),
		schedulerScoreMetaFieldBucketSize, strconv.Itoa(bucketSize),
	).Err(); err != nil {
		return err
	}
	if err := m.rdb.Expire(ctx, scoresKey, schedulerScoreInactiveVersionTTL).Err(); err != nil {
		return err
	}
	if err := m.rdb.Expire(ctx, metaKey, schedulerScoreInactiveVersionTTL).Err(); err != nil {
		return err
	}

	activeKey := schedulerScoreActiveKey(bucket)
	oldActive, err := m.rdb.Get(ctx, activeKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	hasOldActive := "0"
	oldScoresKey, oldMetaKey := activeKey, activeKey
	if oldActive != "" {
		hasOldActive = "1"
		oldScoresKey = schedulerScoreScoresKey(bucket, oldActive)
		oldMetaKey = schedulerScoreMetaKey(bucket, oldActive)
	}
	// active 切换必须和 PERSIST 新版本同一个脚本完成，避免 active 指向仍带临时 TTL 的版本。
	_, err = schedulerScoreActivateScript.Run(ctx, m.rdb,
		[]string{activeKey, scoresKey, metaKey, oldScoresKey, oldMetaKey},
		versionText, int(schedulerScoreOldActiveVersionTTL.Seconds()), hasOldActive,
	).Result()
	return err
}

func (m *schedulerScoreReadModel) DeleteBucket(ctx context.Context, bucket service.SchedulerBucket) error {
	if m == nil || m.rdb == nil {
		return nil
	}
	activeKey := schedulerScoreActiveKey(bucket)
	active, err := m.rdb.Get(ctx, activeKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	keys := []string{activeKey}
	if active != "" {
		keys = append(keys, schedulerScoreScoresKey(bucket, active), schedulerScoreMetaKey(bucket, active))
	}
	return m.rdb.Del(ctx, keys...).Err()
}

func (m *noopSchedulerScoreReadModel) NextVersion(context.Context, service.SchedulerBucket) (int64, error) {
	return 0, nil
}

func (m *noopSchedulerScoreReadModel) GetScores(context.Context, service.SchedulerBucket, []int64) (map[int64]service.SchedulerScoreSnapshot, error) {
	return map[int64]service.SchedulerScoreSnapshot{}, nil
}

func (m *noopSchedulerScoreReadModel) SetBucketScores(context.Context, service.SchedulerBucket, []service.SchedulerScoreSnapshot, int64, time.Time) error {
	return nil
}

func (m *noopSchedulerScoreReadModel) DeleteBucket(context.Context, service.SchedulerBucket) error {
	return nil
}

func schedulerScoreAccountIDFields(accountIDs []int64) []string {
	seen := make(map[int64]struct{}, len(accountIDs))
	fields := make([]string, 0, len(accountIDs))
	for _, accountID := range accountIDs {
		if accountID <= 0 {
			continue
		}
		if _, ok := seen[accountID]; ok {
			continue
		}
		seen[accountID] = struct{}{}
		fields = append(fields, strconv.FormatInt(accountID, 10))
	}
	return fields
}

func schedulerScoreHashTag(bucket service.SchedulerBucket) string {
	return "{bucket:" + bucket.String() + "}"
}

func schedulerScoreVersionKey(bucket service.SchedulerBucket) string {
	return schedulerScoreRedisKeyPrefix + ":" + schedulerScoreHashTag(bucket) + ":version"
}

func schedulerScoreActiveKey(bucket service.SchedulerBucket) string {
	return schedulerScoreRedisKeyPrefix + ":" + schedulerScoreHashTag(bucket) + ":active"
}

func schedulerScoreScoresKey(bucket service.SchedulerBucket, version string) string {
	return schedulerScoreRedisKeyPrefix + ":" + schedulerScoreHashTag(bucket) + ":scores:" + version
}

func schedulerScoreMetaKey(bucket service.SchedulerBucket, version string) string {
	return schedulerScoreRedisKeyPrefix + ":" + schedulerScoreHashTag(bucket) + ":meta:" + version
}
