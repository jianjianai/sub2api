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
	schedulerCandidateIndexWriteChunkSize = 1000
	schedulerCandidateIndexReadChunkSize  = 512
	schedulerCandidateIndexTempTTL        = 10 * time.Minute
	schedulerCandidateIndexOldVersionTTL  = 5 * time.Minute
)

type schedulerCandidateIndexReadModel struct {
	rdb *redis.Client
}

type noopSchedulerCandidateIndexReadModel struct{}

func NewSchedulerCandidateIndexReadModel(rdb *redis.Client) service.SchedulerCandidateIndexReadModel {
	if rdb == nil {
		return &noopSchedulerCandidateIndexReadModel{}
	}
	return &schedulerCandidateIndexReadModel{rdb: rdb}
}

func (n *noopSchedulerCandidateIndexReadModel) NextVersion(ctx context.Context, bucket service.SchedulerBucket) (int64, error) {
	return 0, nil
}

func (n *noopSchedulerCandidateIndexReadModel) GetCandidatePage(ctx context.Context, bucket service.SchedulerBucket, start int, limit int) (service.SchedulerCandidatePage, error) {
	return service.SchedulerCandidatePage{Start: start, End: start, Hit: false}, nil
}

func (n *noopSchedulerCandidateIndexReadModel) SetBucketCandidates(ctx context.Context, bucket service.SchedulerBucket, accounts []service.Account, version int64, updatedAt time.Time) error {
	return nil
}

func (n *noopSchedulerCandidateIndexReadModel) DeleteBucket(ctx context.Context, bucket service.SchedulerBucket) error {
	return nil
}

func (r *schedulerCandidateIndexReadModel) NextVersion(ctx context.Context, bucket service.SchedulerBucket) (int64, error) {
	if r == nil || r.rdb == nil {
		return 0, nil
	}
	return r.rdb.Incr(ctx, schedulerCandidateVersionKey(bucket)).Result()
}

func (r *schedulerCandidateIndexReadModel) GetCandidatePage(ctx context.Context, bucket service.SchedulerBucket, start int, limit int) (service.SchedulerCandidatePage, error) {
	page := service.SchedulerCandidatePage{Start: start, End: start}
	if r == nil || r.rdb == nil {
		return page, nil
	}
	if start < 0 {
		start = 0
	}
	if limit <= 0 {
		limit = schedulerCandidateIndexReadChunkSize
	}
	page.Start = start

	active, err := r.rdb.Get(ctx, schedulerCandidateActiveKey(bucket)).Result()
	if errors.Is(err, redis.Nil) || active == "" {
		return page, nil
	}
	if err != nil {
		return page, err
	}
	version, err := strconv.ParseInt(active, 10, 64)
	if err != nil || version <= 0 {
		return page, nil
	}

	meta, err := r.getCandidateMeta(ctx, bucket, version)
	if err != nil {
		return page, err
	}
	end := start + limit - 1
	ids, err := r.rdb.ZRange(ctx, schedulerCandidateIDsKey(bucket, version), int64(start), int64(end)).Result()
	if err != nil {
		return page, err
	}
	page.Meta = meta
	page.Hit = true
	page.HasMore = start+limit < meta.BucketSize
	if len(ids) == 0 {
		page.End = start
		return page, nil
	}

	values, err := r.rdb.HMGet(ctx, schedulerCandidateAccountsKey(bucket, version), ids...).Result()
	if err != nil {
		return page, err
	}
	accounts := make([]*service.Account, 0, len(values))
	for i, value := range values {
		raw, ok := value.(string)
		if !ok || raw == "" {
			continue
		}
		var account service.Account
		if err := json.Unmarshal([]byte(raw), &account); err != nil {
			slog.Warn("scheduler_candidate_index_decode_failed", "bucket", bucket.String(), "version", version, "account_id", ids[i], "error", err)
			continue
		}
		accounts = append(accounts, &account)
	}
	page.Accounts = accounts
	page.End = start + len(ids)
	return page, nil
}

func (r *schedulerCandidateIndexReadModel) SetBucketCandidates(ctx context.Context, bucket service.SchedulerBucket, accounts []service.Account, version int64, updatedAt time.Time) error {
	if r == nil || r.rdb == nil {
		return nil
	}
	if version <= 0 {
		return fmt.Errorf("scheduler candidate index version must be positive")
	}
	accounts = service.BuildSchedulerCandidateIndexAccounts(bucket, accounts)

	oldActive, err := r.rdb.Get(ctx, schedulerCandidateActiveKey(bucket)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	idsKey := schedulerCandidateIDsKey(bucket, version)
	accountsKey := schedulerCandidateAccountsKey(bucket, version)
	metaKey := schedulerCandidateMetaKey(bucket, version)
	if err := r.rdb.Del(ctx, idsKey, accountsKey, metaKey).Err(); err != nil {
		return err
	}
	for start := 0; start < len(accounts); start += schedulerCandidateIndexWriteChunkSize {
		end := start + schedulerCandidateIndexWriteChunkSize
		if end > len(accounts) {
			end = len(accounts)
		}
		if err := r.writeCandidateChunk(ctx, bucket, version, accounts[start:end], start); err != nil {
			return err
		}
	}

	if err := r.rdb.HSet(ctx, metaKey, map[string]any{
		"version":     strconv.FormatInt(version, 10),
		"updated_at":  updatedAt.Format(time.RFC3339Nano),
		"bucket_size": strconv.Itoa(len(accounts)),
	}).Err(); err != nil {
		return err
	}
	pipe := r.rdb.Pipeline()
	pipe.Expire(ctx, idsKey, schedulerCandidateIndexTempTTL)
	pipe.Expire(ctx, accountsKey, schedulerCandidateIndexTempTTL)
	pipe.Expire(ctx, metaKey, schedulerCandidateIndexTempTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	oldVersion, _ := strconv.ParseInt(oldActive, 10, 64)
	if err := switchSchedulerCandidateActiveVersion(ctx, r.rdb, bucket, version, oldVersion); err != nil {
		return err
	}
	return nil
}

func (r *schedulerCandidateIndexReadModel) DeleteBucket(ctx context.Context, bucket service.SchedulerBucket) error {
	if r == nil || r.rdb == nil {
		return nil
	}
	active, err := r.rdb.Get(ctx, schedulerCandidateActiveKey(bucket)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	keys := []string{schedulerCandidateActiveKey(bucket)}
	if active != "" {
		if version, parseErr := strconv.ParseInt(active, 10, 64); parseErr == nil && version > 0 {
			keys = append(keys,
				schedulerCandidateIDsKey(bucket, version),
				schedulerCandidateAccountsKey(bucket, version),
				schedulerCandidateMetaKey(bucket, version),
			)
		}
	}
	return r.rdb.Del(ctx, keys...).Err()
}

func (r *schedulerCandidateIndexReadModel) writeCandidateChunk(ctx context.Context, bucket service.SchedulerBucket, version int64, accounts []service.Account, offset int) error {
	if len(accounts) == 0 {
		return nil
	}
	pipe := r.rdb.Pipeline()
	ids := make([]redis.Z, 0, len(accounts))
	values := make(map[string]any, len(accounts))
	for i, account := range accounts {
		accountID := strconv.FormatInt(account.ID, 10)
		raw, err := json.Marshal(account)
		if err != nil {
			return err
		}
		ids = append(ids, redis.Z{
			Score:  float64(offset + i),
			Member: accountID,
		})
		values[accountID] = string(raw)
	}
	pipe.ZAdd(ctx, schedulerCandidateIDsKey(bucket, version), ids...)
	pipe.HSet(ctx, schedulerCandidateAccountsKey(bucket, version), values)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *schedulerCandidateIndexReadModel) getCandidateMeta(ctx context.Context, bucket service.SchedulerBucket, version int64) (service.SchedulerCandidateIndexMeta, error) {
	values, err := r.rdb.HGetAll(ctx, schedulerCandidateMetaKey(bucket, version)).Result()
	if err != nil {
		return service.SchedulerCandidateIndexMeta{}, err
	}
	meta := service.SchedulerCandidateIndexMeta{Bucket: bucket, Version: version}
	if raw := values["bucket_size"]; raw != "" {
		meta.BucketSize, _ = strconv.Atoi(raw)
	}
	if raw := values["updated_at"]; raw != "" {
		meta.UpdatedAt, _ = time.Parse(time.RFC3339Nano, raw)
	}
	return meta, nil
}

func switchSchedulerCandidateActiveVersion(ctx context.Context, rdb *redis.Client, bucket service.SchedulerBucket, version int64, oldVersion int64) error {
	keys := []string{
		schedulerCandidateActiveKey(bucket),
		schedulerCandidateIDsKey(bucket, version),
		schedulerCandidateAccountsKey(bucket, version),
		schedulerCandidateMetaKey(bucket, version),
		schedulerCandidateIDsKey(bucket, oldVersion),
		schedulerCandidateAccountsKey(bucket, oldVersion),
		schedulerCandidateMetaKey(bucket, oldVersion),
	}
	_, err := schedulerCandidateSwitchScript.Run(ctx, rdb, keys,
		strconv.FormatInt(version, 10),
		strconv.FormatInt(oldVersion, 10),
		int(schedulerCandidateIndexOldVersionTTL.Seconds()),
	).Result()
	return err
}

var schedulerCandidateSwitchScript = redis.NewScript(`
local active = redis.call("GET", KEYS[1])
local current = tonumber(active or "0") or 0
local new_version = tonumber(ARGV[1])
local old_version = tonumber(ARGV[2])
local old_ttl = tonumber(ARGV[3])
if new_version < current then
  return 0
end
redis.call("PERSIST", KEYS[2])
redis.call("PERSIST", KEYS[3])
redis.call("PERSIST", KEYS[4])
redis.call("SET", KEYS[1], ARGV[1])
if old_version > 0 and old_version ~= new_version then
  redis.call("EXPIRE", KEYS[5], old_ttl)
  redis.call("EXPIRE", KEYS[6], old_ttl)
  redis.call("EXPIRE", KEYS[7], old_ttl)
end
return 1
`)

func schedulerCandidateBaseKey(bucket service.SchedulerBucket) string {
	return "scheduler_candidate:v1:{bucket:" + bucket.String() + "}"
}

func schedulerCandidateVersionKey(bucket service.SchedulerBucket) string {
	return schedulerCandidateBaseKey(bucket) + ":version"
}

func schedulerCandidateActiveKey(bucket service.SchedulerBucket) string {
	return schedulerCandidateBaseKey(bucket) + ":active"
}

func schedulerCandidateIDsKey(bucket service.SchedulerBucket, version int64) string {
	return schedulerCandidateBaseKey(bucket) + ":ids:" + strconv.FormatInt(version, 10)
}

func schedulerCandidateAccountsKey(bucket service.SchedulerBucket, version int64) string {
	return schedulerCandidateBaseKey(bucket) + ":accounts:" + strconv.FormatInt(version, 10)
}

func schedulerCandidateMetaKey(bucket service.SchedulerBucket, version int64) string {
	return schedulerCandidateBaseKey(bucket) + ":meta:" + strconv.FormatInt(version, 10)
}
