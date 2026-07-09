package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const (
	schedulerBucketSetKey         = "sched:buckets"
	schedulerOutboxWatermarkKey   = "sched:outbox:watermark"
	schedulerAccountPrefix        = "sched:acc:"
	schedulerAccountMetaPrefix    = "sched:meta:"
	schedulerActivePrefix         = "sched:active:"
	schedulerReadyPrefix          = "sched:ready:"
	schedulerVersionPrefix        = "sched:ver:"
	schedulerSnapshotPrefix       = "sched:"
	schedulerLockPrefix           = "sched:lock:"
	schedulerCandidatePrefix      = "sched:cand:"
	schedulerCandidateReadyPrefix = "sched:cand-ready:"
	schedulerCandidateCountPrefix = "sched:cand-count:"
	schedulerAccountBucketsPrefix = "sched:acc-buckets:"
	schedulerBlockedKey           = "sched:blocked"
	schedulerBlockPrefix          = "sched:block:"
	schedulerRestoreLockPrefix    = "sched:restore-lock:"

	defaultSchedulerSnapshotMGetChunkSize  = 128
	defaultSchedulerSnapshotWriteChunkSize = 256

	// snapshotGraceTTLSeconds 旧快照过期的宽限期（秒）。
	// 替代立即 DEL，让正在读取旧版本的 reader 有足够时间完成 ZRANGE。
	snapshotGraceTTLSeconds = 60
)

var (
	// activateSnapshotScript 原子 CAS 切换快照版本。
	// 仅当新版本号 >= 当前激活版本时才切换，防止并发写入导致版本回滚。
	// 旧快照使用 EXPIRE 设置宽限期而非立即 DEL，避免与 reader 竞态。
	//
	// KEYS[1] = activeKey     (sched:active:{bucket})
	// KEYS[2] = readyKey      (sched:ready:{bucket})
	// KEYS[3] = bucketSetKey  (sched:buckets)
	// KEYS[4] = snapshotKey   (新写入的快照 key)
	// ARGV[1] = 新版本号字符串
	// ARGV[2] = bucket 字符串 (用于 SADD)
	// ARGV[3] = 快照 key 前缀 (用于构造旧快照 key)
	// ARGV[4] = 宽限期 TTL 秒数
	//
	// 返回 1 = 已激活, 0 = 版本过旧未激活
	activateSnapshotScript = redis.NewScript(`
local currentActive = redis.call('GET', KEYS[1])
local newVersion = tonumber(ARGV[1])

if currentActive ~= false then
	local curVersion = tonumber(currentActive)
	if curVersion and newVersion < curVersion then
		redis.call('DEL', KEYS[4])
		return 0
	end
end

redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], '1')
redis.call('SADD', KEYS[3], ARGV[2])

if currentActive ~= false and currentActive ~= ARGV[1] then
	redis.call('EXPIRE', ARGV[3] .. currentActive, tonumber(ARGV[4]))
end

return 1
`)

	adjustCandidateCountScript = redis.NewScript(`
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local delta = tonumber(ARGV[1])
local nextValue = current + delta
if nextValue < 0 then
	nextValue = 0
end
redis.call('SET', KEYS[1], tostring(nextValue))
return nextValue
`)
)

type schedulerCache struct {
	rdb            *redis.Client
	mgetChunkSize  int
	writeChunkSize int
}

func NewSchedulerCache(rdb *redis.Client) service.SchedulerCache {
	return newSchedulerCacheWithChunkSizes(rdb, defaultSchedulerSnapshotMGetChunkSize, defaultSchedulerSnapshotWriteChunkSize)
}

func newSchedulerCacheWithChunkSizes(rdb *redis.Client, mgetChunkSize, writeChunkSize int) service.SchedulerCache {
	if mgetChunkSize <= 0 {
		mgetChunkSize = defaultSchedulerSnapshotMGetChunkSize
	}
	if writeChunkSize <= 0 {
		writeChunkSize = defaultSchedulerSnapshotWriteChunkSize
	}
	return &schedulerCache{
		rdb:            rdb,
		mgetChunkSize:  mgetChunkSize,
		writeChunkSize: writeChunkSize,
	}
}

func (c *schedulerCache) GetSnapshot(ctx context.Context, bucket service.SchedulerBucket) ([]*service.Account, bool, error) {
	readyKey := schedulerBucketKey(schedulerReadyPrefix, bucket)
	readyVal, err := c.rdb.Get(ctx, readyKey).Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if readyVal != "1" {
		return nil, false, nil
	}

	activeKey := schedulerBucketKey(schedulerActivePrefix, bucket)
	activeVal, err := c.rdb.Get(ctx, activeKey).Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	snapshotKey := schedulerSnapshotKey(bucket, activeVal)
	ids, err := c.rdb.ZRange(ctx, snapshotKey, 0, -1).Result()
	if err != nil {
		return nil, false, err
	}
	if len(ids) == 0 {
		// 空快照视为缓存未命中，触发数据库回退查询
		// 这解决了新分组创建后立即绑定账号时的竞态条件问题
		return nil, false, nil
	}

	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, schedulerAccountMetaKey(id))
	}
	values, err := c.mgetChunked(ctx, keys)
	if err != nil {
		return nil, false, err
	}

	accounts := make([]*service.Account, 0, len(values))
	for _, val := range values {
		if val == nil {
			return nil, false, nil
		}
		account, err := decodeCachedAccount(val)
		if err != nil {
			return nil, false, err
		}
		accounts = append(accounts, account)
	}

	return accounts, true, nil
}

// SetSnapshot 是旧调度结构性 rebuild 的入口：保留旧版 versioned snapshot，
// 同时刷新 candidate index，供未切换或回滚场景保持缓存一致。
func (c *schedulerCache) SetSnapshot(ctx context.Context, bucket service.SchedulerBucket, accounts []service.Account) error {
	versionKey := schedulerBucketKey(schedulerVersionPrefix, bucket)
	version, err := c.rdb.Incr(ctx, versionKey).Result()
	if err != nil {
		return err
	}

	versionStr := strconv.FormatInt(version, 10)
	snapshotKey := schedulerSnapshotKey(bucket, versionStr)

	if err := c.writeAccounts(ctx, accounts); err != nil {
		return err
	}

	if len(accounts) > 0 {
		members := make([]redis.Z, 0, len(accounts))
		for idx, account := range accounts {
			members = append(members, redis.Z{
				Score:  float64(idx),
				Member: strconv.FormatInt(account.ID, 10),
			})
		}
		pipe := c.rdb.Pipeline()
		for start := 0; start < len(members); start += c.writeChunkSize {
			end := start + c.writeChunkSize
			if end > len(members) {
				end = len(members)
			}
			pipe.ZAdd(ctx, snapshotKey, members[start:end]...)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}

	activeKey := schedulerBucketKey(schedulerActivePrefix, bucket)
	readyKey := schedulerBucketKey(schedulerReadyPrefix, bucket)
	snapshotKeyPrefix := fmt.Sprintf("%s%d:%s:%s:v", schedulerSnapshotPrefix, bucket.GroupID, bucket.Platform, bucket.Mode)

	keys := []string{activeKey, readyKey, schedulerBucketSetKey, snapshotKey}
	args := []any{versionStr, bucket.String(), snapshotKeyPrefix, snapshotGraceTTLSeconds}

	activatedRaw, err := activateSnapshotScript.Run(ctx, c.rdb, keys, args...).Result()
	if err != nil {
		return err
	}
	if activated, ok := activatedRaw.(int64); ok && activated == 0 {
		return nil
	}

	// 快照 CAS 激活成功后再替换 candidate index，避免旧版本 rebuild 覆盖新版本候选集。
	return c.SetCandidateIndex(ctx, bucket, accounts)
}

// SetCandidateIndex 写入 candidate-only 索引：只更新账号快照、候选 zset、
// candidate ready 和反向索引，不写旧 snapshot active/ready/version key。
func (c *schedulerCache) SetCandidateIndex(ctx context.Context, bucket service.SchedulerBucket, accounts []service.Account) error {
	if err := c.writeAccounts(ctx, accounts); err != nil {
		return err
	}

	buildID := strconv.FormatInt(time.Now().UnixNano(), 10)
	candidateKey := schedulerCandidateKey(bucket)
	tmpCandidateKey := fmt.Sprintf("%s:tmp:%s", candidateKey, buildID)
	newCandidateIDs := make(map[string]struct{})
	candidateMembers := make([]redis.Z, 0, len(accounts))
	for _, account := range accounts {
		if !account.IsSchedulable() {
			continue
		}
		id := strconv.FormatInt(account.ID, 10)
		newCandidateIDs[id] = struct{}{}
		candidateMembers = append(candidateMembers, redis.Z{
			Score:  schedulerCandidateScore(account, bucket),
			Member: id,
		})
	}
	if err := c.rdb.Del(ctx, tmpCandidateKey).Err(); err != nil {
		return err
	}
	if len(candidateMembers) > 0 {
		pipe := c.rdb.Pipeline()
		for start := 0; start < len(candidateMembers); start += c.writeChunkSize {
			end := start + c.writeChunkSize
			if end > len(candidateMembers) {
				end = len(candidateMembers)
			}
			pipe.ZAdd(ctx, tmpCandidateKey, candidateMembers[start:end]...)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}

	// 只有 build/rebuild 路径允许读取旧 candidate 全量成员，用于维护反向索引 diff；
	// 请求热路径的 ListCandidateAccounts 禁止 ZRANGE 0 -1。
	oldCandidateIDs, err := c.rdb.ZRange(ctx, candidateKey, 0, -1).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	if err := c.replaceCandidateIndex(ctx, bucket, candidateKey, tmpCandidateKey, candidateMembers, oldCandidateIDs, newCandidateIDs); err != nil {
		return err
	}
	// count 不是统计指标，而是完整性标记：ready=1 且 count>0 但 zset 不存在时，
	// 请求侧必须把它当成索引丢失并触发构建；count=0 才能把无 zset 解释为空 bucket。
	pipe := c.rdb.Pipeline()
	pipe.Set(ctx, schedulerCandidateCountKey(bucket), strconv.Itoa(len(candidateMembers)), 0)
	pipe.Set(ctx, schedulerBucketKey(schedulerCandidateReadyPrefix, bucket), "1", 0)
	_, err = pipe.Exec(ctx)
	return err
}

func (c *schedulerCache) DeleteOldSnapshots(ctx context.Context, buckets []service.SchedulerBucket) error {
	seen := make(map[string]service.SchedulerBucket, len(buckets))
	for _, bucket := range buckets {
		seen[bucket.String()] = bucket
	}

	pipe := c.rdb.Pipeline()
	for _, bucket := range seen {
		pipe.Del(ctx,
			schedulerBucketKey(schedulerActivePrefix, bucket),
			schedulerBucketKey(schedulerReadyPrefix, bucket),
			schedulerBucketKey(schedulerVersionPrefix, bucket),
		)
		pattern := fmt.Sprintf("%s%d:%s:%s:v*", schedulerSnapshotPrefix, bucket.GroupID, bucket.Platform, bucket.Mode)
		if err := c.scanDelete(ctx, pattern); err != nil {
			return err
		}
	}
	for _, bucket := range seen {
		pipe.SRem(ctx, schedulerBucketSetKey, bucket.String())
	}
	_, err := pipe.Exec(ctx)
	return err
}

// replaceCandidateIndex 原子切换当前 bucket 的候选集合，并按 diff 维护
// sched:acc-buckets:{id} 反向索引，供状态性 block 低成本删除候选。
func (c *schedulerCache) replaceCandidateIndex(
	ctx context.Context,
	bucket service.SchedulerBucket,
	candidateKey string,
	tmpCandidateKey string,
	candidateMembers []redis.Z,
	oldCandidateIDs []string,
	newCandidateIDs map[string]struct{},
) error {
	oldSet := make(map[string]struct{}, len(oldCandidateIDs))
	for _, id := range oldCandidateIDs {
		oldSet[id] = struct{}{}
	}

	if len(candidateMembers) > 0 {
		if err := c.rdb.Rename(ctx, tmpCandidateKey, candidateKey).Err(); err != nil {
			return err
		}
	} else if err := c.rdb.Del(ctx, candidateKey, tmpCandidateKey).Err(); err != nil {
		return err
	}

	bucketName := bucket.String()
	pipe := c.rdb.Pipeline()
	for id := range oldSet {
		if _, retained := newCandidateIDs[id]; retained {
			continue
		}
		pipe.SRem(ctx, schedulerAccountBucketsKey(id), bucketName)
	}
	for id := range newCandidateIDs {
		pipe.SAdd(ctx, schedulerAccountBucketsKey(id), bucketName)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// ListCandidateAccounts 是选号热路径：只读固定小批量候选 ID 和对应 meta，
// 不访问 DB、不重建 bucket、不读取整个候选集合。
func (c *schedulerCache) ListCandidateAccounts(ctx context.Context, bucket service.SchedulerBucket, opts service.SchedulerCandidateListOptions) ([]*service.Account, bool, error) {
	readyVal, err := c.rdb.Get(ctx, schedulerBucketKey(schedulerCandidateReadyPrefix, bucket)).Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if readyVal != "1" {
		return nil, false, nil
	}

	expectedCount, hit, err := c.readCandidateCount(ctx, bucket)
	if err != nil {
		return nil, false, err
	}
	if !hit {
		// ready 存在但 count 缺失说明不是完整 candidate index。不要把它当成空 bucket；
		// 上层会触发 targeted build 并等待 ready，避免数据丢失时静默返回 no available。
		return nil, false, nil
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = service.DefaultSchedulerCandidateFetchLimit
	}
	if limit < service.MinSchedulerCandidateFetchLimit {
		limit = service.MinSchedulerCandidateFetchLimit
	}

	candidateKey := schedulerCandidateKey(bucket)
	exists, err := c.rdb.Exists(ctx, candidateKey).Result()
	if err != nil {
		return nil, false, err
	}
	if exists == 0 {
		if expectedCount <= 0 {
			// 空 bucket 也是有效缓存命中，避免空分组在请求路径反复触发构建。
			return []*service.Account{}, true, nil
		}
		return nil, false, nil
	}

	for attempt := 0; attempt < 3; attempt++ {
		ids, err := c.rdb.ZRange(ctx, candidateKey, 0, int64(limit-1)).Result()
		if err != nil {
			return nil, false, err
		}
		if len(ids) == 0 {
			if expectedCount <= 0 {
				return []*service.Account{}, true, nil
			}
			return nil, false, nil
		}

		keys := make([]string, 0, len(ids))
		for _, id := range ids {
			keys = append(keys, schedulerAccountMetaKey(id))
		}
		values, err := c.mgetChunked(ctx, keys)
		if err != nil {
			return nil, false, err
		}

		missing := make([]any, 0)
		accounts := make([]*service.Account, 0, len(values))
		for i, val := range values {
			if val == nil {
				missing = append(missing, ids[i])
				continue
			}
			account, err := decodeCachedAccount(val)
			if err != nil {
				return nil, false, err
			}
			accounts = append(accounts, account)
		}
		if len(missing) > 0 {
			// meta 缺失通常说明局部缓存损坏。这里只清理坏 member，最多补两轮，
			// 仍然不在请求路径访问数据库或触发 rebuild。
			slog.Warn("scheduler_candidate_index_corrupt_meta_missing",
				"bucket", bucket.String(),
				"missing_count", len(missing),
			)
			if err := c.rdb.ZRem(ctx, candidateKey, missing...).Err(); err != nil {
				return nil, false, err
			}
			if err := c.incrementCandidateCount(ctx, bucket, -int64(len(missing))); err != nil {
				return nil, false, err
			}
			if len(accounts) == 0 && attempt < 2 {
				continue
			}
		}
		return accounts, true, nil
	}
	return nil, false, nil
}

// RemoveAccountFromCandidates 处理状态性不可调度：通过反向索引找到账号所在 bucket，
// 只做 ZREM/SREM 和 blocked 队列写入，不扫描全部 bucket。
func (c *schedulerCache) RemoveAccountFromCandidates(ctx context.Context, accountID int64, state service.SchedulerBlockedAccountState) ([]service.SchedulerBucket, error) {
	if accountID <= 0 {
		return nil, nil
	}
	id := strconv.FormatInt(accountID, 10)
	rawBuckets, err := c.rdb.SMembers(ctx, schedulerAccountBucketsKey(id)).Result()
	if err != nil {
		return nil, err
	}

	buckets := make([]service.SchedulerBucket, 0, len(rawBuckets))
	type removedCandidate struct {
		bucket service.SchedulerBucket
		cmd    *redis.IntCmd
	}
	removedCandidates := make([]removedCandidate, 0, len(rawBuckets))
	pipe := c.rdb.Pipeline()
	for _, raw := range rawBuckets {
		bucket, ok := service.ParseSchedulerBucket(raw)
		if !ok {
			continue
		}
		buckets = append(buckets, bucket)
		cmd := pipe.ZRem(ctx, schedulerCandidateKey(bucket), id)
		removedCandidates = append(removedCandidates, removedCandidate{bucket: bucket, cmd: cmd})
	}
	pipe.Del(ctx, schedulerAccountBucketsKey(id))
	c.enqueueBlockedAccount(ctx, pipe, accountID, state)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	for _, removed := range removedCandidates {
		if removed.cmd.Val() <= 0 {
			continue
		}
		if err := c.incrementCandidateCount(ctx, removed.bucket, -removed.cmd.Val()); err != nil {
			return nil, err
		}
	}
	if len(buckets) == 0 {
		// 反向索引 miss 不做全量补救；缺失通常意味着索引尚未初始化或账号本就不在候选中。
		slog.Warn("scheduler_candidate_reverse_index_miss",
			"account_id", accountID,
			"reason", state.Reason,
			"source", state.Source,
		)
	}
	return buckets, nil
}

// RestoreAccountCandidates 只恢复调用方已确认结构归属的账号；内部仍用
// IsSchedulable 做兜底，避免并发 block/restore 把不可调度账号加回候选池。
func (c *schedulerCache) RestoreAccountCandidates(ctx context.Context, account *service.Account, buckets []service.SchedulerBucket) error {
	if account == nil || account.ID <= 0 {
		return nil
	}
	if !account.IsSchedulable() {
		return nil
	}
	if err := c.writeAccounts(ctx, []service.Account{*account}); err != nil {
		return err
	}

	id := strconv.FormatInt(account.ID, 10)
	type addedCandidate struct {
		bucket service.SchedulerBucket
		cmd    *redis.IntCmd
	}
	addedCandidates := make([]addedCandidate, 0, len(buckets))
	pipe := c.rdb.Pipeline()
	for _, bucket := range buckets {
		cmd := pipe.ZAdd(ctx, schedulerCandidateKey(bucket), redis.Z{
			Score:  schedulerCandidateScore(*account, bucket),
			Member: id,
		})
		addedCandidates = append(addedCandidates, addedCandidate{bucket: bucket, cmd: cmd})
		pipe.SAdd(ctx, schedulerAccountBucketsKey(id), bucket.String())
	}
	pipe.ZRem(ctx, schedulerBlockedKey, id)
	pipe.Del(ctx, schedulerBlockKey(id))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	for _, added := range addedCandidates {
		if added.cmd.Val() <= 0 {
			continue
		}
		if err := c.incrementCandidateCount(ctx, added.bucket, added.cmd.Val()); err != nil {
			return err
		}
	}
	return nil
}

// UpdateCandidateScores 批量更新 LastUsedAt 和候选 zset score。
// 它只由 deferred/batch 路径调用，避免每个请求同步写候选分数。
func (c *schedulerCache) UpdateCandidateScores(ctx context.Context, updates map[int64]time.Time) error {
	if len(updates) == 0 {
		return nil
	}

	for id, lastUsedAt := range updates {
		if err := c.updateCandidateScore(ctx, id, lastUsedAt); err != nil {
			return err
		}
	}
	return nil
}

func (c *schedulerCache) updateCandidateScore(ctx context.Context, accountID int64, lastUsedAt time.Time) error {
	if accountID <= 0 {
		return nil
	}
	id := strconv.FormatInt(accountID, 10)
	accountVal, err := c.rdb.Get(ctx, schedulerAccountKey(id)).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	account, err := decodeCachedAccount(accountVal)
	if err != nil {
		return err
	}
	account.LastUsedAt = ptrTime(lastUsedAt)

	rawBuckets, err := c.rdb.SMembers(ctx, schedulerAccountBucketsKey(id)).Result()
	if err != nil {
		return err
	}

	fullPayload, err := json.Marshal(account)
	if err != nil {
		return err
	}
	metaPayload, err := json.Marshal(buildSchedulerMetadataAccount(*account))
	if err != nil {
		return err
	}

	pipe := c.rdb.Pipeline()
	pipe.Set(ctx, schedulerAccountKey(id), fullPayload, 0)
	pipe.Set(ctx, schedulerAccountMetaKey(id), metaPayload, 0)
	for _, raw := range rawBuckets {
		bucket, ok := service.ParseSchedulerBucket(raw)
		if !ok {
			continue
		}
		pipe.ZAddXX(ctx, schedulerCandidateKey(bucket), redis.Z{
			Score:  schedulerCandidateScore(*account, bucket),
			Member: id,
		})
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (c *schedulerCache) PopDueBlockedAccounts(ctx context.Context, now time.Time, limit int) ([]int64, error) {
	if limit <= 0 {
		limit = service.DefaultSchedulerCandidateRestoreBatch
	}
	raw, err := c.rdb.ZRangeByScore(ctx, schedulerBlockedKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(now.Unix(), 10),
		Offset: 0,
		Count:  int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(raw))
	for _, item := range raw {
		id, err := strconv.ParseInt(item, 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (c *schedulerCache) AckBlockedAccount(ctx context.Context, accountID int64) error {
	if accountID <= 0 {
		return nil
	}
	id := strconv.FormatInt(accountID, 10)
	pipe := c.rdb.Pipeline()
	pipe.ZRem(ctx, schedulerBlockedKey, id)
	pipe.Del(ctx, schedulerBlockKey(id))
	_, err := pipe.Exec(ctx)
	return err
}

func (c *schedulerCache) RequeueBlockedAccount(ctx context.Context, accountID int64, until time.Time, reason string) error {
	if accountID <= 0 {
		return nil
	}
	pipe := c.rdb.Pipeline()
	c.enqueueBlockedAccount(ctx, pipe, accountID, service.SchedulerBlockedAccountState{
		AccountID: accountID,
		Until:     until,
		Reason:    reason,
		Source:    "requeue",
		UpdatedAt: time.Now(),
	})
	_, err := pipe.Exec(ctx)
	return err
}

func (c *schedulerCache) GetAccount(ctx context.Context, accountID int64) (*service.Account, error) {
	key := schedulerAccountKey(strconv.FormatInt(accountID, 10))
	val, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeCachedAccount(val)
}

func (c *schedulerCache) SetAccount(ctx context.Context, account *service.Account) error {
	if account == nil || account.ID <= 0 {
		return nil
	}
	return c.writeAccounts(ctx, []service.Account{*account})
}

func (c *schedulerCache) DeleteAccount(ctx context.Context, accountID int64) error {
	if accountID <= 0 {
		return nil
	}
	id := strconv.FormatInt(accountID, 10)
	return c.rdb.Del(ctx, schedulerAccountKey(id), schedulerAccountMetaKey(id)).Err()
}

func (c *schedulerCache) UpdateLastUsed(ctx context.Context, updates map[int64]time.Time) error {
	return c.UpdateCandidateScores(ctx, updates)
}

func (c *schedulerCache) TryLockBucket(ctx context.Context, bucket service.SchedulerBucket, ttl time.Duration) (bool, error) {
	key := schedulerBucketKey(schedulerLockPrefix, bucket)
	return c.rdb.SetNX(ctx, key, time.Now().UnixNano(), ttl).Result()
}

func (c *schedulerCache) UnlockBucket(ctx context.Context, bucket service.SchedulerBucket) error {
	key := schedulerBucketKey(schedulerLockPrefix, bucket)
	return c.rdb.Del(ctx, key).Err()
}

func (c *schedulerCache) ListBuckets(ctx context.Context) ([]service.SchedulerBucket, error) {
	raw, err := c.rdb.SMembers(ctx, schedulerBucketSetKey).Result()
	if err != nil {
		return nil, err
	}
	out := make([]service.SchedulerBucket, 0, len(raw))
	for _, entry := range raw {
		bucket, ok := service.ParseSchedulerBucket(entry)
		if !ok {
			continue
		}
		out = append(out, bucket)
	}
	return out, nil
}

func (c *schedulerCache) GetOutboxWatermark(ctx context.Context) (int64, error) {
	val, err := c.rdb.Get(ctx, schedulerOutboxWatermarkKey).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (c *schedulerCache) SetOutboxWatermark(ctx context.Context, id int64) error {
	return c.rdb.Set(ctx, schedulerOutboxWatermarkKey, strconv.FormatInt(id, 10), 0).Err()
}

func schedulerBucketKey(prefix string, bucket service.SchedulerBucket) string {
	return fmt.Sprintf("%s%d:%s:%s", prefix, bucket.GroupID, bucket.Platform, bucket.Mode)
}

func schedulerSnapshotKey(bucket service.SchedulerBucket, version string) string {
	return fmt.Sprintf("%s%d:%s:%s:v%s", schedulerSnapshotPrefix, bucket.GroupID, bucket.Platform, bucket.Mode, version)
}

func schedulerCandidateKey(bucket service.SchedulerBucket) string {
	return schedulerBucketKey(schedulerCandidatePrefix, bucket)
}

func schedulerCandidateCountKey(bucket service.SchedulerBucket) string {
	return schedulerBucketKey(schedulerCandidateCountPrefix, bucket)
}

func schedulerAccountKey(id string) string {
	return schedulerAccountPrefix + id
}

func schedulerAccountMetaKey(id string) string {
	return schedulerAccountMetaPrefix + id
}

func schedulerAccountBucketsKey(id string) string {
	return schedulerAccountBucketsPrefix + id
}

func schedulerBlockKey(id string) string {
	return schedulerBlockPrefix + id
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func schedulerCandidateScore(account service.Account, bucket service.SchedulerBucket) float64 {
	priority := account.Priority
	if bucket.GroupID > 0 {
		for _, group := range account.AccountGroups {
			if group.GroupID == bucket.GroupID {
				priority = group.Priority
				break
			}
		}
	}
	lastUsedMillis := int64(0)
	if account.LastUsedAt != nil {
		lastUsedMillis = account.LastUsedAt.UnixMilli()
	}
	return float64(priority)*10000000000000 + float64(lastUsedMillis)
}

type schedulerBlockedAccountPayload struct {
	AccountID     int64  `json:"account_id"`
	UntilUnix     int64  `json:"until_unix"`
	Reason        string `json:"reason"`
	Source        string `json:"source"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

// enqueueBlockedAccount 写 blocked 延迟队列和调试 payload。
// 删除候选后账号不会自动回池，必须由立即恢复路径或 worker 根据 DB 最新状态恢复。
func (c *schedulerCache) enqueueBlockedAccount(ctx context.Context, pipe redis.Pipeliner, accountID int64, state service.SchedulerBlockedAccountState) {
	if accountID <= 0 {
		return
	}
	now := time.Now()
	until := state.Until
	if until.IsZero() || !until.After(now) {
		until = now.Add(time.Minute)
	}
	updatedAt := state.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}
	reason := state.Reason
	if reason == "" {
		reason = "blocked"
	}
	source := state.Source
	if source == "" {
		source = "scheduler_cache"
	}

	id := strconv.FormatInt(accountID, 10)
	payload, err := json.Marshal(schedulerBlockedAccountPayload{
		AccountID:     accountID,
		UntilUnix:     until.Unix(),
		Reason:        reason,
		Source:        source,
		UpdatedAtUnix: updatedAt.Unix(),
	})
	if err != nil {
		return
	}
	ttl := until.Sub(now) + time.Hour
	if ttl < time.Hour {
		ttl = time.Hour
	}
	pipe.ZAdd(ctx, schedulerBlockedKey, redis.Z{
		Score:  float64(until.Unix()),
		Member: id,
	})
	pipe.Set(ctx, schedulerBlockKey(id), payload, ttl)
}

func decodeCachedAccount(val any) (*service.Account, error) {
	var payload []byte
	switch raw := val.(type) {
	case string:
		payload = []byte(raw)
	case []byte:
		payload = raw
	default:
		return nil, fmt.Errorf("unexpected account cache type: %T", val)
	}
	var account service.Account
	if err := json.Unmarshal(payload, &account); err != nil {
		return nil, err
	}
	return &account, nil
}

func (c *schedulerCache) writeAccounts(ctx context.Context, accounts []service.Account) error {
	if len(accounts) == 0 {
		return nil
	}

	pipe := c.rdb.Pipeline()
	pending := 0
	flush := func() error {
		if pending == 0 {
			return nil
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		pipe = c.rdb.Pipeline()
		pending = 0
		return nil
	}

	for _, account := range accounts {
		fullPayload, err := json.Marshal(account)
		if err != nil {
			return err
		}
		metaPayload, err := json.Marshal(buildSchedulerMetadataAccount(account))
		if err != nil {
			return err
		}

		id := strconv.FormatInt(account.ID, 10)
		pipe.Set(ctx, schedulerAccountKey(id), fullPayload, 0)
		pipe.Set(ctx, schedulerAccountMetaKey(id), metaPayload, 0)
		pending++
		if pending >= c.writeChunkSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	return flush()
}

func (c *schedulerCache) mgetChunked(ctx context.Context, keys []string) ([]any, error) {
	if len(keys) == 0 {
		return []any{}, nil
	}

	out := make([]any, 0, len(keys))
	chunkSize := c.mgetChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultSchedulerSnapshotMGetChunkSize
	}
	for start := 0; start < len(keys); start += chunkSize {
		end := start + chunkSize
		if end > len(keys) {
			end = len(keys)
		}
		part, err := c.rdb.MGet(ctx, keys[start:end]...).Result()
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
	}
	return out, nil
}

func (c *schedulerCache) readCandidateCount(ctx context.Context, bucket service.SchedulerBucket) (int64, bool, error) {
	raw, err := c.rdb.Get(ctx, schedulerCandidateCountKey(bucket)).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	count, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return count, true, nil
}

func (c *schedulerCache) incrementCandidateCount(ctx context.Context, bucket service.SchedulerBucket, delta int64) error {
	if delta == 0 {
		return nil
	}
	return adjustCandidateCountScript.Run(ctx, c.rdb, []string{schedulerCandidateCountKey(bucket)}, delta).Err()
}

func (c *schedulerCache) scanDelete(ctx context.Context, pattern string) error {
	var cursor uint64
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := c.rdb.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func buildSchedulerMetadataAccount(account service.Account) service.Account {
	return service.Account{
		ID:                      account.ID,
		Name:                    account.Name,
		Platform:                account.Platform,
		Type:                    account.Type,
		Concurrency:             account.Concurrency,
		LoadFactor:              account.LoadFactor,
		Priority:                account.Priority,
		RateMultiplier:          account.RateMultiplier,
		Status:                  account.Status,
		LastUsedAt:              account.LastUsedAt,
		ExpiresAt:               account.ExpiresAt,
		AutoPauseOnExpired:      account.AutoPauseOnExpired,
		Schedulable:             account.Schedulable,
		RateLimitedAt:           account.RateLimitedAt,
		RateLimitResetAt:        account.RateLimitResetAt,
		OverloadUntil:           account.OverloadUntil,
		TempUnschedulableUntil:  account.TempUnschedulableUntil,
		TempUnschedulableReason: account.TempUnschedulableReason,
		SessionWindowStart:      account.SessionWindowStart,
		SessionWindowEnd:        account.SessionWindowEnd,
		SessionWindowStatus:     account.SessionWindowStatus,
		ParentAccountID:         account.ParentAccountID,
		QuotaDimension:          account.QuotaDimension,
		AccountGroups:           filterSchedulerAccountGroups(account.AccountGroups),
		GroupIDs:                filterSchedulerGroupIDs(account.GroupIDs, account.AccountGroups),
		Credentials:             filterSchedulerCredentials(account.Credentials),
		Extra:                   filterSchedulerExtra(account.Extra),
	}
}

func filterSchedulerAccountGroups(accountGroups []service.AccountGroup) []service.AccountGroup {
	if len(accountGroups) == 0 {
		return nil
	}

	filtered := make([]service.AccountGroup, 0, len(accountGroups))
	for _, ag := range accountGroups {
		if ag.GroupID <= 0 {
			continue
		}
		filtered = append(filtered, service.AccountGroup{
			AccountID: ag.AccountID,
			GroupID:   ag.GroupID,
			Priority:  ag.Priority,
			CreatedAt: ag.CreatedAt,
		})
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func filterSchedulerGroupIDs(groupIDs []int64, accountGroups []service.AccountGroup) []int64 {
	if len(groupIDs) == 0 && len(accountGroups) == 0 {
		return nil
	}

	seen := make(map[int64]struct{}, len(groupIDs)+len(accountGroups))
	filtered := make([]int64, 0, len(groupIDs)+len(accountGroups))
	for _, id := range groupIDs {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		filtered = append(filtered, id)
	}
	for _, ag := range accountGroups {
		if ag.GroupID <= 0 {
			continue
		}
		if _, ok := seen[ag.GroupID]; ok {
			continue
		}
		seen[ag.GroupID] = struct{}{}
		filtered = append(filtered, ag.GroupID)
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func filterSchedulerCredentials(credentials map[string]any) map[string]any {
	if len(credentials) == 0 {
		return nil
	}
	keys := []string{"model_mapping", "compact_model_mapping", "api_key", "project_id", "oauth_type", "plan_type"}
	filtered := make(map[string]any)
	for _, key := range keys {
		if value, ok := credentials[key]; ok && value != nil {
			filtered[key] = value
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func filterSchedulerExtra(extra map[string]any) map[string]any {
	if len(extra) == 0 {
		return nil
	}
	keys := []string{
		"mixed_scheduling",
		"window_cost_limit",
		"window_cost_sticky_reserve",
		"max_sessions",
		"session_idle_timeout_minutes",
		"openai_oauth_responses_websockets_v2_enabled",
		"openai_oauth_responses_websockets_v2_mode",
		"openai_apikey_responses_websockets_v2_enabled",
		"openai_apikey_responses_websockets_v2_mode",
		"responses_websockets_v2_enabled",
		"openai_ws_enabled",
		"openai_ws_force_http",
		"openai_responses_mode",
		"openai_responses_supported",
		"codex_5h_used_percent",
		"codex_7d_used_percent",
		"codex_5h_reset_at",
		"codex_7d_reset_at",
		"codex_5h_reset_after_seconds",
		"codex_7d_reset_after_seconds",
		"codex_usage_updated_at",
		"auto_pause_5h_threshold",
		"auto_pause_7d_threshold",
		"auto_pause_5h_disabled",
		"auto_pause_7d_disabled",
		"model_rate_limits",
	}
	filtered := make(map[string]any)
	for _, key := range keys {
		if value, ok := extra[key]; ok && value != nil {
			filtered[key] = value
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}
