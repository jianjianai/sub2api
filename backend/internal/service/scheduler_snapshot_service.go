package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

var (
	ErrSchedulerCacheNotReady   = errors.New("scheduler cache not ready")
	ErrSchedulerFallbackLimited = errors.New("scheduler db fallback limited")
)

const (
	outboxEventTimeout          = 2 * time.Minute
	schedulerOutboxCleanupBatch = 5000
	fullRebuildLockTTL          = 10 * time.Minute
	fullRebuildJobPollInterval  = 5 * time.Second
)

// batchSeenKey tracks which (groupID, platform) bucket sets have already been
// rebuilt within a single pollOutbox call, to avoid redundant work when multiple
// account_changed events share the same groups.
type batchSeenKey struct {
	groupID  int64
	platform string
}

type SchedulerSnapshotService struct {
	cache             SchedulerCache
	outboxRepo        SchedulerOutboxRepository
	rebuildJobRepo    SchedulerRebuildJobRepository
	accountRepo       AccountRepository
	groupRepo         GroupRepository
	cfg               *config.Config
	stopCh            chan struct{}
	stopOnce          sync.Once
	wg                sync.WaitGroup
	fallbackLimit     *fallbackLimiter
	workerID          string
	fullRebuildRunner func(context.Context, string) error
}

func NewSchedulerSnapshotService(
	cache SchedulerCache,
	outboxRepo SchedulerOutboxRepository,
	accountRepo AccountRepository,
	groupRepo GroupRepository,
	cfg *config.Config,
	rebuildJobRepo ...SchedulerRebuildJobRepository,
) *SchedulerSnapshotService {
	maxQPS := 0
	if cfg != nil {
		maxQPS = cfg.Gateway.Scheduling.DbFallbackMaxQPS
	}
	var jobRepo SchedulerRebuildJobRepository
	if len(rebuildJobRepo) > 0 {
		jobRepo = rebuildJobRepo[0]
	}
	return &SchedulerSnapshotService{
		cache:          cache,
		outboxRepo:     outboxRepo,
		rebuildJobRepo: jobRepo,
		accountRepo:    accountRepo,
		groupRepo:      groupRepo,
		cfg:            cfg,
		stopCh:         make(chan struct{}),
		fallbackLimit:  newFallbackLimiter(maxQPS),
		workerID:       schedulerWorkerID(),
	}
}

func (s *SchedulerSnapshotService) Start() {
	if s == nil || s.cache == nil {
		return
	}

	if s.rebuildJobRepo != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.enqueueStartupRebuild()
		}()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.runFullRebuildJobWorker()
		}()
	}

	interval := s.outboxPollInterval()
	if s.outboxRepo != nil && interval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.runOutboxWorker(interval)
		}()
	}

	fullInterval := s.fullRebuildInterval()
	if s.rebuildJobRepo != nil && fullInterval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.runFullRebuildWorker(fullInterval)
		}()
	}
}

func (s *SchedulerSnapshotService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
}

func (s *SchedulerSnapshotService) ListSchedulableAccounts(ctx context.Context, groupID *int64, platform string, hasForcePlatform bool) ([]Account, bool, error) {
	useMixed := (platform == PlatformAnthropic || platform == PlatformGemini) && !hasForcePlatform
	mode := s.resolveMode(platform, hasForcePlatform)
	bucket := s.bucketFor(groupID, platform, mode)

	if s.cache != nil {
		cached, hit, err := s.cache.GetSnapshot(ctx, bucket)
		if err != nil {
			logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] cache read failed: bucket=%s err=%v", bucket.String(), err)
		} else if hit {
			return derefAccounts(cached), useMixed, nil
		}
	}

	if err := s.guardFallback(ctx); err != nil {
		return nil, useMixed, err
	}

	fallbackCtx, cancel := s.withFallbackTimeout(ctx)
	defer cancel()

	accounts, err := s.loadAccountsFromDB(fallbackCtx, bucket, useMixed)
	if err != nil {
		return nil, useMixed, err
	}

	if s.cache != nil {
		if err := s.cache.SetSnapshot(fallbackCtx, bucket, accounts); err != nil {
			logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] cache write failed: bucket=%s err=%v", bucket.String(), err)
		}
	}

	return accounts, useMixed, nil
}

func (s *SchedulerSnapshotService) GetAccount(ctx context.Context, accountID int64) (*Account, error) {
	if accountID <= 0 {
		return nil, nil
	}
	if s.cache != nil {
		account, err := s.cache.GetAccount(ctx, accountID)
		if err != nil {
			logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] account cache read failed: id=%d err=%v", accountID, err)
		} else if account != nil {
			return account, nil
		}
	}

	if err := s.guardFallback(ctx); err != nil {
		return nil, err
	}
	fallbackCtx, cancel := s.withFallbackTimeout(ctx)
	defer cancel()
	return s.accountRepo.GetByID(fallbackCtx, accountID)
}

// GetGroupByID 获取分组信息（供调度器使用）
func (s *SchedulerSnapshotService) GetGroupByID(ctx context.Context, groupID int64) (*Group, error) {
	if s.groupRepo == nil {
		return nil, nil
	}
	return s.groupRepo.GetByID(ctx, groupID)
}

// UpdateAccountInCache 立即更新 Redis 中单个账号的数据（用于模型限流后立即生效）
func (s *SchedulerSnapshotService) UpdateAccountInCache(ctx context.Context, account *Account) error {
	if s.cache == nil || account == nil {
		return nil
	}
	return s.cache.SetAccount(ctx, account)
}

func (s *SchedulerSnapshotService) enqueueStartupRebuild() {
	if s.rebuildJobRepo == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.rebuildJobRepo.EnqueueFullRebuild(ctx, "startup", "startup", nil); err != nil {
		logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] startup full rebuild enqueue failed: %v", err)
	}
}

func (s *SchedulerSnapshotService) runOutboxWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.pollOutbox()
	for {
		select {
		case <-ticker.C:
			s.pollOutbox()
		case <-s.stopCh:
			return
		}
	}
}

func (s *SchedulerSnapshotService) runFullRebuildWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			backlog, err := s.currentOutboxBacklog(ctx)
			if err == nil && backlog > 0 {
				logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] interval full rebuild skipped because outbox backlog is positive: backlog=%d", backlog)
				cancel()
				continue
			}
			if err := s.rebuildJobRepo.EnqueueFullRebuild(ctx, "interval", "interval", nil); err != nil {
				logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] interval full rebuild enqueue failed: %v", err)
			}
			cancel()
		case <-s.stopCh:
			return
		}
	}
}

func (s *SchedulerSnapshotService) runFullRebuildJobWorker() {
	ticker := time.NewTicker(fullRebuildJobPollInterval)
	defer ticker.Stop()

	for {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			processed := s.processOneFullRebuildJob(ctx)
			cancel()
			if !processed {
				break
			}
		}

		select {
		case <-ticker.C:
		case <-s.stopCh:
			return
		}
	}
}

func (s *SchedulerSnapshotService) processOneFullRebuildJob(ctx context.Context) bool {
	if s == nil || s.rebuildJobRepo == nil || s.cache == nil {
		return false
	}
	job, err := s.rebuildJobRepo.ClaimNextFullRebuild(ctx, s.workerID, fullRebuildLockTTL)
	if err != nil {
		logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] full rebuild job claim failed: %v", err)
		return false
	}
	if job == nil {
		return false
	}

	ok, err := s.cache.TryLockFullRebuild(ctx, fullRebuildLockTTL)
	if err != nil {
		s.markFullRebuildJobFailed(job, err)
		return true
	}
	if !ok {
		s.markFullRebuildJobFailed(job, errors.New("full rebuild lock is held"))
		return true
	}
	defer func() {
		_ = s.cache.UnlockFullRebuild(context.Background())
	}()

	runner := s.fullRebuildRunner
	if runner == nil {
		runner = s.runFullRebuildNow
	}
	if err := runner(ctx, job.Reason); err != nil {
		s.markFullRebuildJobFailed(job, err)
		return true
	}

	markCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.rebuildJobRepo.MarkFullRebuildSucceeded(markCtx, job.ID); err != nil {
		logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] full rebuild job success mark failed: id=%d err=%v", job.ID, err)
	}
	return true
}

func (s *SchedulerSnapshotService) markFullRebuildJobFailed(job *SchedulerRebuildJob, cause error) {
	if s == nil || s.rebuildJobRepo == nil || job == nil {
		return
	}
	runAfter := time.Now().Add(fullRebuildBackoff(job.Attempts))
	markCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.rebuildJobRepo.MarkFullRebuildFailed(markCtx, job.ID, truncateSchedulerString(cause.Error(), 2048), runAfter); err != nil {
		logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] full rebuild job failure mark failed: id=%d err=%v", job.ID, err)
	}
}

func (s *SchedulerSnapshotService) pollOutbox() {
	if s.outboxRepo == nil || s.cache == nil {
		return
	}

	deadline := time.Now().Add(s.outboxDrainMaxDuration())
	maxBatches := s.outboxDrainMaxBatchesPerTick()
	batchSize := s.outboxPollBatchSize()

	var oldestForObserve *SchedulerOutboxEvent
	var watermarkForObserve int64

	for batch := 0; batch < maxBatches; batch++ {
		if time.Now().After(deadline) {
			break
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		watermark, err := s.cache.GetOutboxWatermark(ctx)
		if err != nil {
			cancel()
			logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] outbox watermark read failed: %v", err)
			return
		}

		events, err := s.outboxRepo.ListAfterAndReleaseDedup(ctx, watermark, batchSize)
		cancel()
		if err != nil {
			logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] outbox poll failed: %v", err)
			return
		}
		if len(events) == 0 {
			break
		}

		if oldestForObserve == nil {
			copy := events[0]
			oldestForObserve = &copy
		}

		eventCtx, eventCancel := context.WithTimeout(context.Background(), outboxEventTimeout)
		err = s.processOutboxBatch(eventCtx, events)
		eventCancel()
		if err != nil {
			logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] outbox handle failed: first_id=%d last_id=%d err=%v", events[0].ID, events[len(events)-1].ID, err)
			return
		}

		lastID := events[len(events)-1].ID
		if err := s.writeOutboxWatermarkWithRetry(lastID); err != nil {
			logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] outbox watermark write failed: %v", err)
			return
		}
		watermarkForObserve = lastID
		s.cleanupConsumedOutboxAsync(lastID)

		if len(events) < batchSize {
			break
		}
	}

	if oldestForObserve != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		s.observeOutboxLag(ctx, *oldestForObserve, watermarkForObserve)
		cancel()
	}
}

func (s *SchedulerSnapshotService) writeOutboxWatermarkWithRetry(id int64) error {
	var wmErr error
	for i := range 3 {
		wmCtx, wmCancel := context.WithTimeout(context.Background(), 5*time.Second)
		wmErr = s.cache.SetOutboxWatermark(wmCtx, id)
		wmCancel()
		if wmErr == nil {
			return nil
		}
		if i < 2 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	return wmErr
}

func (s *SchedulerSnapshotService) cleanupConsumedOutboxAsync(watermark int64) {
	if s == nil || watermark <= 0 {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.cleanupConsumedOutbox(watermark)
	}()
}

func (s *SchedulerSnapshotService) cleanupConsumedOutbox(watermark int64) {
	if s == nil || s.outboxRepo == nil || watermark <= 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lease, acquired, err := s.outboxRepo.TryAcquireCleanupLock(ctx)
	if err != nil {
		logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] outbox cleanup lock failed: %v", err)
		return
	}
	if !acquired {
		return
	}
	defer lease.Release()

	for {
		deleted, err := s.outboxRepo.DeleteConsumedUpTo(ctx, watermark, schedulerOutboxCleanupBatch)
		if err != nil {
			logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] outbox cleanup failed: watermark=%d err=%v", watermark, err)
			return
		}
		if deleted == 0 || deleted < schedulerOutboxCleanupBatch {
			return
		}
	}
}

type outboxBatchPlan struct {
	lastUsed             map[int64]time.Time
	accountIDs           map[int64]struct{}
	allPlatformGroupIDs  map[int64]struct{}
	platformGroupIDs     map[string]map[int64]struct{}
	fullRebuildOutboxIDs []int64
}

func (s *SchedulerSnapshotService) processOutboxBatch(ctx context.Context, events []SchedulerOutboxEvent) error {
	if len(events) == 0 {
		return nil
	}
	plan := outboxBatchPlan{
		lastUsed:            make(map[int64]time.Time),
		accountIDs:          make(map[int64]struct{}),
		allPlatformGroupIDs: make(map[int64]struct{}),
		platformGroupIDs:    make(map[string]map[int64]struct{}),
	}

	for _, event := range events {
		switch event.EventType {
		case SchedulerOutboxEventAccountLastUsed:
			mergeLastUsed(plan.lastUsed, event.Payload)
		case SchedulerOutboxEventAccountBulkChanged:
			for _, id := range parseInt64Slice(event.Payload["account_ids"]) {
				if id > 0 {
					plan.accountIDs[id] = struct{}{}
				}
			}
			if s.accountRepo != nil {
				addGroupIDs(plan.allPlatformGroupIDs, parseInt64Slice(event.Payload["group_ids"]))
			}
		case SchedulerOutboxEventAccountGroupsChanged, SchedulerOutboxEventAccountChanged:
			if event.AccountID != nil && *event.AccountID > 0 {
				plan.accountIDs[*event.AccountID] = struct{}{}
			}
			if s.accountRepo != nil {
				addGroupIDs(plan.allPlatformGroupIDs, parseInt64Slice(event.Payload["group_ids"]))
			}
		case SchedulerOutboxEventGroupChanged:
			if event.GroupID != nil && *event.GroupID > 0 {
				plan.allPlatformGroupIDs[*event.GroupID] = struct{}{}
			}
		case SchedulerOutboxEventFullRebuild:
			plan.fullRebuildOutboxIDs = append(plan.fullRebuildOutboxIDs, event.ID)
		}
	}

	for _, outboxID := range plan.fullRebuildOutboxIDs {
		if s.rebuildJobRepo == nil {
			return errors.New("scheduler rebuild job repository is not configured")
		}
		id := outboxID
		if err := s.rebuildJobRepo.EnqueueFullRebuild(ctx, "outbox", "outbox", &id); err != nil {
			return err
		}
	}

	if len(plan.lastUsed) > 0 && s.cache != nil {
		if err := s.cache.UpdateLastUsed(ctx, plan.lastUsed); err != nil {
			return err
		}
	}

	if len(plan.accountIDs) > 0 && s.accountRepo != nil {
		ids := sortedIDs(plan.accountIDs)
		accounts, err := s.accountRepo.GetByIDs(ctx, ids)
		if err != nil {
			return err
		}

		found := make(map[int64]struct{}, len(accounts))
		for _, account := range accounts {
			if account == nil || account.ID <= 0 {
				continue
			}
			found[account.ID] = struct{}{}
			if s.cache != nil {
				if err := s.cache.SetAccount(ctx, account); err != nil {
					return err
				}
			}
			for _, gid := range account.GroupIDs {
				addPlatformGroupID(plan.platformGroupIDs, account.Platform, gid)
				if account.Platform == PlatformAntigravity && account.IsMixedSchedulingEnabled() {
					addPlatformGroupID(plan.platformGroupIDs, PlatformAnthropic, gid)
					addPlatformGroupID(plan.platformGroupIDs, PlatformGemini, gid)
				}
			}
		}

		if s.cache != nil {
			for _, id := range ids {
				if _, ok := found[id]; ok {
					continue
				}
				if err := s.cache.DeleteAccount(ctx, id); err != nil {
					return err
				}
			}
		}
	}

	seen := make(map[batchSeenKey]struct{})
	if len(plan.allPlatformGroupIDs) > 0 {
		if err := s.rebuildByGroupIDs(ctx, sortedIDs(plan.allPlatformGroupIDs), "outbox_batch", seen); err != nil {
			return err
		}
	}
	for _, platform := range sortedPlatforms(plan.platformGroupIDs) {
		if err := s.rebuildBucketsForPlatform(ctx, platform, sortedIDs(plan.platformGroupIDs[platform]), "outbox_batch", seen); err != nil {
			return err
		}
	}
	return nil
}

func mergeLastUsed(out map[int64]time.Time, payload map[string]any) {
	if payload == nil {
		return
	}
	raw, ok := payload["last_used"].(map[string]any)
	if !ok || len(raw) == 0 {
		return
	}
	for key, value := range raw {
		id, err := strconv.ParseInt(key, 10, 64)
		if err != nil || id <= 0 {
			continue
		}
		sec, ok := toInt64(value)
		if !ok || sec <= 0 {
			continue
		}
		usedAt := time.Unix(sec, 0)
		if existing, ok := out[id]; !ok || usedAt.After(existing) {
			out[id] = usedAt
		}
	}
}

func addGroupIDs(dst map[int64]struct{}, ids []int64) {
	for _, id := range ids {
		if id > 0 {
			dst[id] = struct{}{}
		}
	}
}

func addPlatformGroupID(dst map[string]map[int64]struct{}, platform string, groupID int64) {
	if platform == "" || groupID <= 0 {
		return
	}
	if dst[platform] == nil {
		dst[platform] = make(map[int64]struct{})
	}
	dst[platform][groupID] = struct{}{}
}

func sortedIDs(set map[int64]struct{}) []int64 {
	ids := make([]int64, 0, len(set))
	for id := range set {
		if id > 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func sortedPlatforms(groups map[string]map[int64]struct{}) []string {
	platforms := make([]string, 0, len(groups))
	for platform, ids := range groups {
		if platform != "" && len(ids) > 0 {
			platforms = append(platforms, platform)
		}
	}
	sort.Strings(platforms)
	return platforms
}

func (s *SchedulerSnapshotService) rebuildByAccount(ctx context.Context, account *Account, groupIDs []int64, reason string, seen map[batchSeenKey]struct{}) error {
	if account == nil {
		return nil
	}
	groupIDs = s.normalizeGroupIDs(groupIDs)
	if len(groupIDs) == 0 {
		return nil
	}

	var firstErr error
	if err := s.rebuildBucketsForPlatform(ctx, account.Platform, groupIDs, reason, seen); err != nil && firstErr == nil {
		firstErr = err
	}
	if account.Platform == PlatformAntigravity && account.IsMixedSchedulingEnabled() {
		if err := s.rebuildBucketsForPlatform(ctx, PlatformAnthropic, groupIDs, reason, seen); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := s.rebuildBucketsForPlatform(ctx, PlatformGemini, groupIDs, reason, seen); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *SchedulerSnapshotService) rebuildByGroupIDs(ctx context.Context, groupIDs []int64, reason string, seen map[batchSeenKey]struct{}) error {
	groupIDs = s.normalizeGroupIDs(groupIDs)
	if len(groupIDs) == 0 {
		return nil
	}
	platforms := []string{PlatformAnthropic, PlatformGemini, PlatformOpenAI, PlatformAntigravity, PlatformGrok}
	var firstErr error
	for _, platform := range platforms {
		if err := s.rebuildBucketsForPlatform(ctx, platform, groupIDs, reason, seen); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *SchedulerSnapshotService) rebuildBucketsForPlatform(ctx context.Context, platform string, groupIDs []int64, reason string, seen map[batchSeenKey]struct{}) error {
	if platform == "" {
		return nil
	}
	var firstErr error
	for _, gid := range groupIDs {
		// Within a single poll batch, skip (groupID, platform) pairs that were
		// already rebuilt. The first rebuild loads fresh DB data for all accounts
		// in the group, so subsequent rebuilds for the same group+platform within
		// the same batch are redundant.
		if seen != nil {
			key := batchSeenKey{gid, platform}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
		}
		if err := s.rebuildBucket(ctx, SchedulerBucket{GroupID: gid, Platform: platform, Mode: SchedulerModeSingle}, reason); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := s.rebuildBucket(ctx, SchedulerBucket{GroupID: gid, Platform: platform, Mode: SchedulerModeForced}, reason); err != nil && firstErr == nil {
			firstErr = err
		}
		if platform == PlatformAnthropic || platform == PlatformGemini {
			if err := s.rebuildBucket(ctx, SchedulerBucket{GroupID: gid, Platform: platform, Mode: SchedulerModeMixed}, reason); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (s *SchedulerSnapshotService) rebuildBuckets(ctx context.Context, buckets []SchedulerBucket, reason string) error {
	var firstErr error
	for _, bucket := range buckets {
		if err := s.rebuildBucket(ctx, bucket, reason); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *SchedulerSnapshotService) rebuildBucket(ctx context.Context, bucket SchedulerBucket, reason string) error {
	if s.cache == nil {
		return ErrSchedulerCacheNotReady
	}
	ok, err := s.cache.TryLockBucket(ctx, bucket, 30*time.Second)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	defer func() {
		_ = s.cache.UnlockBucket(ctx, bucket)
	}()

	rebuildCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	accounts, err := s.loadAccountsFromDB(rebuildCtx, bucket, bucket.Mode == SchedulerModeMixed)
	if err != nil {
		logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] rebuild failed: bucket=%s reason=%s err=%v", bucket.String(), reason, err)
		return err
	}
	if err := s.cache.SetSnapshot(rebuildCtx, bucket, accounts); err != nil {
		logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] rebuild cache failed: bucket=%s reason=%s err=%v", bucket.String(), reason, err)
		return err
	}
	slog.Debug("[Scheduler] rebuild ok", "bucket", bucket.String(), "reason", reason, "size", len(accounts))
	return nil
}

func (s *SchedulerSnapshotService) runFullRebuildNow(ctx context.Context, reason string) error {
	if s.cache == nil {
		return ErrSchedulerCacheNotReady
	}
	buckets, err := s.cache.ListBuckets(ctx)
	if err != nil {
		logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] list buckets failed: %v", err)
		return err
	}
	if len(buckets) == 0 {
		buckets, err = s.defaultBuckets(ctx)
		if err != nil {
			logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] default buckets failed: %v", err)
			return err
		}
	}
	return s.rebuildBuckets(ctx, buckets, reason)
}

func (s *SchedulerSnapshotService) observeOutboxLag(ctx context.Context, oldest SchedulerOutboxEvent, watermark int64) {
	if oldest.CreatedAt.IsZero() || s.cfg == nil {
		return
	}

	lag := time.Since(oldest.CreatedAt)
	if lagSeconds := int(lag.Seconds()); lagSeconds >= s.cfg.Gateway.Scheduling.OutboxLagWarnSeconds && s.cfg.Gateway.Scheduling.OutboxLagWarnSeconds > 0 {
		logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] outbox lag warning: %ds", lagSeconds)
	}

	threshold := s.cfg.Gateway.Scheduling.OutboxBacklogRebuildRows
	if threshold <= 0 || s.outboxRepo == nil {
		return
	}
	maxID, err := s.outboxRepo.MaxID(ctx)
	if err != nil {
		return
	}
	if maxID-watermark >= int64(threshold) {
		logger.LegacyPrintf("service.scheduler_snapshot", "[Scheduler] outbox backlog warning: backlog=%d", maxID-watermark)
	}
}

func (s *SchedulerSnapshotService) currentOutboxBacklog(ctx context.Context) (int64, error) {
	if s == nil || s.cache == nil || s.outboxRepo == nil {
		return 0, nil
	}
	watermark, err := s.cache.GetOutboxWatermark(ctx)
	if err != nil {
		return 0, err
	}
	maxID, err := s.outboxRepo.MaxID(ctx)
	if err != nil {
		return 0, err
	}
	if maxID <= watermark {
		return 0, nil
	}
	return maxID - watermark, nil
}

func (s *SchedulerSnapshotService) loadAccountsFromDB(ctx context.Context, bucket SchedulerBucket, useMixed bool) ([]Account, error) {
	if s.accountRepo == nil {
		return nil, ErrSchedulerCacheNotReady
	}
	groupID := bucket.GroupID
	if s.isRunModeSimple() {
		groupID = 0
	}

	if useMixed {
		platforms := []string{bucket.Platform, PlatformAntigravity}
		var accounts []Account
		var err error
		if groupID > 0 {
			accounts, err = s.accountRepo.ListSchedulableByGroupIDAndPlatforms(ctx, groupID, platforms)
		} else if s.isRunModeSimple() {
			accounts, err = s.accountRepo.ListSchedulableByPlatforms(ctx, platforms)
		} else {
			accounts, err = s.accountRepo.ListSchedulableUngroupedByPlatforms(ctx, platforms)
		}
		if err != nil {
			return nil, err
		}
		filtered := make([]Account, 0, len(accounts))
		for _, acc := range accounts {
			if acc.Platform == PlatformAntigravity && !acc.IsMixedSchedulingEnabled() {
				continue
			}
			filtered = append(filtered, acc)
		}
		return filtered, nil
	}

	if groupID > 0 {
		return s.accountRepo.ListSchedulableByGroupIDAndPlatform(ctx, groupID, bucket.Platform)
	}
	if s.isRunModeSimple() {
		return s.accountRepo.ListSchedulableByPlatform(ctx, bucket.Platform)
	}
	return s.accountRepo.ListSchedulableUngroupedByPlatform(ctx, bucket.Platform)
}

func (s *SchedulerSnapshotService) bucketFor(groupID *int64, platform string, mode string) SchedulerBucket {
	return SchedulerBucket{
		GroupID:  s.normalizeGroupID(groupID),
		Platform: platform,
		Mode:     mode,
	}
}

func (s *SchedulerSnapshotService) normalizeGroupID(groupID *int64) int64 {
	if s.isRunModeSimple() {
		return 0
	}
	if groupID == nil || *groupID <= 0 {
		return 0
	}
	return *groupID
}

func (s *SchedulerSnapshotService) normalizeGroupIDs(groupIDs []int64) []int64 {
	if s.isRunModeSimple() {
		return []int64{0}
	}
	if len(groupIDs) == 0 {
		return []int64{0}
	}
	seen := make(map[int64]struct{}, len(groupIDs))
	out := make([]int64, 0, len(groupIDs))
	for _, id := range groupIDs {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == 0 {
		return []int64{0}
	}
	return out
}

func (s *SchedulerSnapshotService) resolveMode(platform string, hasForcePlatform bool) string {
	if hasForcePlatform {
		return SchedulerModeForced
	}
	if platform == PlatformAnthropic || platform == PlatformGemini {
		return SchedulerModeMixed
	}
	return SchedulerModeSingle
}

func (s *SchedulerSnapshotService) guardFallback(ctx context.Context) error {
	if s.cfg == nil || s.cfg.Gateway.Scheduling.DbFallbackEnabled {
		if s.fallbackLimit == nil || s.fallbackLimit.Allow() {
			return nil
		}
		return ErrSchedulerFallbackLimited
	}
	return ErrSchedulerCacheNotReady
}

func (s *SchedulerSnapshotService) withFallbackTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.cfg == nil || s.cfg.Gateway.Scheduling.DbFallbackTimeoutSeconds <= 0 {
		return context.WithCancel(ctx)
	}
	timeout := time.Duration(s.cfg.Gateway.Scheduling.DbFallbackTimeoutSeconds) * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.WithCancel(ctx)
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	return context.WithTimeout(ctx, timeout)
}

func (s *SchedulerSnapshotService) isRunModeSimple() bool {
	return s.cfg != nil && s.cfg.RunMode == config.RunModeSimple
}

func (s *SchedulerSnapshotService) outboxPollInterval() time.Duration {
	if s.cfg == nil {
		return time.Second
	}
	sec := s.cfg.Gateway.Scheduling.OutboxPollIntervalSeconds
	if sec <= 0 {
		return time.Second
	}
	return time.Duration(sec) * time.Second
}

func (s *SchedulerSnapshotService) outboxPollBatchSize() int {
	if s.cfg == nil || s.cfg.Gateway.Scheduling.OutboxPollBatchSize <= 0 {
		return 1000
	}
	return s.cfg.Gateway.Scheduling.OutboxPollBatchSize
}

func (s *SchedulerSnapshotService) outboxDrainMaxBatchesPerTick() int {
	if s.cfg == nil || s.cfg.Gateway.Scheduling.OutboxDrainMaxBatchesPerTick <= 0 {
		return 8
	}
	return s.cfg.Gateway.Scheduling.OutboxDrainMaxBatchesPerTick
}

func (s *SchedulerSnapshotService) outboxDrainMaxDuration() time.Duration {
	if s.cfg == nil || s.cfg.Gateway.Scheduling.OutboxDrainMaxDurationSeconds <= 0 {
		return 8 * time.Second
	}
	return time.Duration(s.cfg.Gateway.Scheduling.OutboxDrainMaxDurationSeconds) * time.Second
}

func (s *SchedulerSnapshotService) fullRebuildInterval() time.Duration {
	if s.cfg == nil {
		return 0
	}
	sec := s.cfg.Gateway.Scheduling.FullRebuildIntervalSeconds
	if sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}

func fullRebuildBackoff(attempts int) time.Duration {
	switch {
	case attempts <= 1:
		return 30 * time.Second
	case attempts == 2:
		return time.Minute
	case attempts == 3:
		return 2 * time.Minute
	default:
		return 5 * time.Minute
	}
}

func truncateSchedulerString(value string, maxLen int) string {
	if maxLen <= 0 || len(value) <= maxLen {
		return value
	}
	return value[:maxLen]
}

func schedulerWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + ":" + strconv.Itoa(os.Getpid())
}

func (s *SchedulerSnapshotService) defaultBuckets(ctx context.Context) ([]SchedulerBucket, error) {
	buckets := make([]SchedulerBucket, 0)
	platforms := []string{PlatformAnthropic, PlatformGemini, PlatformOpenAI, PlatformAntigravity, PlatformGrok}
	for _, platform := range platforms {
		buckets = append(buckets, SchedulerBucket{GroupID: 0, Platform: platform, Mode: SchedulerModeSingle})
		buckets = append(buckets, SchedulerBucket{GroupID: 0, Platform: platform, Mode: SchedulerModeForced})
		if platform == PlatformAnthropic || platform == PlatformGemini {
			buckets = append(buckets, SchedulerBucket{GroupID: 0, Platform: platform, Mode: SchedulerModeMixed})
		}
	}

	if s.isRunModeSimple() || s.groupRepo == nil {
		return dedupeBuckets(buckets), nil
	}

	groups, err := s.groupRepo.ListActive(ctx)
	if err != nil {
		return dedupeBuckets(buckets), nil
	}
	for _, group := range groups {
		if group.Platform == "" {
			continue
		}
		buckets = append(buckets, SchedulerBucket{GroupID: group.ID, Platform: group.Platform, Mode: SchedulerModeSingle})
		buckets = append(buckets, SchedulerBucket{GroupID: group.ID, Platform: group.Platform, Mode: SchedulerModeForced})
		if group.Platform == PlatformAnthropic || group.Platform == PlatformGemini {
			buckets = append(buckets, SchedulerBucket{GroupID: group.ID, Platform: group.Platform, Mode: SchedulerModeMixed})
		}
	}
	return dedupeBuckets(buckets), nil
}

func dedupeBuckets(in []SchedulerBucket) []SchedulerBucket {
	seen := make(map[string]struct{}, len(in))
	out := make([]SchedulerBucket, 0, len(in))
	for _, bucket := range in {
		key := bucket.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, bucket)
	}
	return out
}

func derefAccounts(accounts []*Account) []Account {
	if len(accounts) == 0 {
		return []Account{}
	}
	out := make([]Account, 0, len(accounts))
	for _, account := range accounts {
		if account == nil {
			continue
		}
		out = append(out, *account)
	}
	return out
}

func parseInt64Slice(value any) []int64 {
	switch raw := value.(type) {
	case []any:
		out := make([]int64, 0, len(raw))
		for _, item := range raw {
			if v, ok := toInt64(item); ok && v > 0 {
				out = append(out, v)
			}
		}
		return out
	case []int64:
		out := make([]int64, 0, len(raw))
		for _, v := range raw {
			if v > 0 {
				out = append(out, v)
			}
		}
		return out
	case []int:
		out := make([]int64, 0, len(raw))
		for _, v := range raw {
			if v > 0 {
				out = append(out, int64(v))
			}
		}
		return out
	default:
		return nil
	}
}

func toInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	case json.Number:
		parsed, err := strconv.ParseInt(v.String(), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

type fallbackLimiter struct {
	maxQPS int
	mu     sync.Mutex
	window time.Time
	count  int
}

func newFallbackLimiter(maxQPS int) *fallbackLimiter {
	if maxQPS <= 0 {
		return nil
	}
	return &fallbackLimiter{
		maxQPS: maxQPS,
		window: time.Now(),
	}
}

func (l *fallbackLimiter) Allow() bool {
	if l == nil || l.maxQPS <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if now.Sub(l.window) >= time.Second {
		l.window = now
		l.count = 0
	}
	if l.count >= l.maxQPS {
		return false
	}
	l.count++
	return true
}
