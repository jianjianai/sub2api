package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type schedulerLoopCache struct {
	watermark          int64
	setWatermarks      []int64
	lastUsed           map[int64]time.Time
	setAccounts        []int64
	deletedAccounts    []int64
	snapshots          []SchedulerBucket
	fullRebuildLockOK  bool
	fullLockAttempts   int
	fullUnlockAttempts int
	listBuckets        []SchedulerBucket
}

func (c *schedulerLoopCache) GetSnapshot(ctx context.Context, bucket SchedulerBucket) ([]*Account, bool, error) {
	return nil, false, nil
}

func (c *schedulerLoopCache) SetSnapshot(ctx context.Context, bucket SchedulerBucket, accounts []Account) error {
	c.snapshots = append(c.snapshots, bucket)
	return nil
}

func (c *schedulerLoopCache) GetAccount(ctx context.Context, accountID int64) (*Account, error) {
	return nil, nil
}

func (c *schedulerLoopCache) SetAccount(ctx context.Context, account *Account) error {
	if account != nil {
		c.setAccounts = append(c.setAccounts, account.ID)
	}
	return nil
}

func (c *schedulerLoopCache) DeleteAccount(ctx context.Context, accountID int64) error {
	c.deletedAccounts = append(c.deletedAccounts, accountID)
	return nil
}

func (c *schedulerLoopCache) UpdateLastUsed(ctx context.Context, updates map[int64]time.Time) error {
	c.lastUsed = updates
	return nil
}

func (c *schedulerLoopCache) TryLockBucket(ctx context.Context, bucket SchedulerBucket, ttl time.Duration) (bool, error) {
	return true, nil
}

func (c *schedulerLoopCache) UnlockBucket(ctx context.Context, bucket SchedulerBucket) error {
	return nil
}

func (c *schedulerLoopCache) TryLockFullRebuild(ctx context.Context, ttl time.Duration) (bool, error) {
	c.fullLockAttempts++
	return c.fullRebuildLockOK, nil
}

func (c *schedulerLoopCache) UnlockFullRebuild(ctx context.Context) error {
	c.fullUnlockAttempts++
	return nil
}

func (c *schedulerLoopCache) ListBuckets(ctx context.Context) ([]SchedulerBucket, error) {
	return c.listBuckets, nil
}

func (c *schedulerLoopCache) GetOutboxWatermark(ctx context.Context) (int64, error) {
	return c.watermark, nil
}

func (c *schedulerLoopCache) SetOutboxWatermark(ctx context.Context, id int64) error {
	c.watermark = id
	c.setWatermarks = append(c.setWatermarks, id)
	return nil
}

type schedulerLoopOutboxRepo struct {
	batches [][]SchedulerOutboxEvent
	maxID   int64
}

func (r *schedulerLoopOutboxRepo) ListAfterAndReleaseDedup(ctx context.Context, afterID int64, limit int) ([]SchedulerOutboxEvent, error) {
	if len(r.batches) == 0 {
		return nil, nil
	}
	events := r.batches[0]
	r.batches = r.batches[1:]
	return events, nil
}

func (r *schedulerLoopOutboxRepo) MaxID(ctx context.Context) (int64, error) {
	return r.maxID, nil
}

func (r *schedulerLoopOutboxRepo) DeleteConsumedUpTo(ctx context.Context, watermark int64, limit int) (int64, error) {
	return 0, nil
}

func (r *schedulerLoopOutboxRepo) TryAcquireCleanupLock(ctx context.Context) (SchedulerOutboxCleanupLease, bool, error) {
	return nil, false, nil
}

type schedulerLoopEnqueue struct {
	reason       string
	source       string
	sourceOutbox *int64
}

type schedulerLoopFailedJob struct {
	id       int64
	errText  string
	runAfter time.Time
}

type schedulerLoopRebuildJobRepo struct {
	mu        sync.Mutex
	enqueued  []schedulerLoopEnqueue
	claims    []*SchedulerRebuildJob
	failed    []schedulerLoopFailedJob
	succeeded []int64
}

func (r *schedulerLoopRebuildJobRepo) EnqueueFullRebuild(ctx context.Context, reason string, source string, sourceOutboxID *int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var copied *int64
	if sourceOutboxID != nil {
		v := *sourceOutboxID
		copied = &v
	}
	r.enqueued = append(r.enqueued, schedulerLoopEnqueue{reason: reason, source: source, sourceOutbox: copied})
	return nil
}

func (r *schedulerLoopRebuildJobRepo) ClaimNextFullRebuild(ctx context.Context, workerID string, lockTTL time.Duration) (*SchedulerRebuildJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.claims) == 0 {
		return nil, nil
	}
	job := r.claims[0]
	r.claims = r.claims[1:]
	return job, nil
}

func (r *schedulerLoopRebuildJobRepo) MarkFullRebuildSucceeded(ctx context.Context, jobID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.succeeded = append(r.succeeded, jobID)
	return nil
}

func (r *schedulerLoopRebuildJobRepo) MarkFullRebuildFailed(ctx context.Context, jobID int64, errText string, runAfter time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed = append(r.failed, schedulerLoopFailedJob{id: jobID, errText: errText, runAfter: runAfter})
	return nil
}

func (r *schedulerLoopRebuildJobRepo) enqueueCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.enqueued)
}

type schedulerLoopAccountRepo struct {
	AccountRepository
	accounts      map[int64]*Account
	getByIDsCalls int
}

func (r *schedulerLoopAccountRepo) GetByIDs(ctx context.Context, ids []int64) ([]*Account, error) {
	r.getByIDsCalls++
	out := make([]*Account, 0, len(ids))
	for _, id := range ids {
		if account := r.accounts[id]; account != nil {
			out = append(out, account)
		}
	}
	return out, nil
}

func (r *schedulerLoopAccountRepo) ListSchedulableByGroupIDAndPlatform(ctx context.Context, groupID int64, platform string) ([]Account, error) {
	return r.listByPlatformAndGroup(platform, groupID), nil
}

func (r *schedulerLoopAccountRepo) ListSchedulableByPlatform(ctx context.Context, platform string) ([]Account, error) {
	return r.listByPlatformAndGroup(platform, 0), nil
}

func (r *schedulerLoopAccountRepo) ListSchedulableUngroupedByPlatform(ctx context.Context, platform string) ([]Account, error) {
	return r.listByPlatformAndGroup(platform, 0), nil
}

func (r *schedulerLoopAccountRepo) ListSchedulableByPlatforms(ctx context.Context, platforms []string) ([]Account, error) {
	return r.listByPlatformsAndGroup(platforms, 0), nil
}

func (r *schedulerLoopAccountRepo) ListSchedulableByGroupIDAndPlatforms(ctx context.Context, groupID int64, platforms []string) ([]Account, error) {
	return r.listByPlatformsAndGroup(platforms, groupID), nil
}

func (r *schedulerLoopAccountRepo) ListSchedulableUngroupedByPlatforms(ctx context.Context, platforms []string) ([]Account, error) {
	return r.listByPlatformsAndGroup(platforms, 0), nil
}

func (r *schedulerLoopAccountRepo) listByPlatformsAndGroup(platforms []string, groupID int64) []Account {
	allowed := make(map[string]struct{}, len(platforms))
	for _, platform := range platforms {
		allowed[platform] = struct{}{}
	}
	var out []Account
	for _, account := range r.accounts {
		if account == nil {
			continue
		}
		if _, ok := allowed[account.Platform]; !ok {
			continue
		}
		if groupID > 0 && !hasGroup(account.GroupIDs, groupID) {
			continue
		}
		out = append(out, *account)
	}
	return out
}

func (r *schedulerLoopAccountRepo) listByPlatformAndGroup(platform string, groupID int64) []Account {
	return r.listByPlatformsAndGroup([]string{platform}, groupID)
}

func hasGroup(groupIDs []int64, groupID int64) bool {
	for _, id := range groupIDs {
		if id == groupID {
			return true
		}
	}
	return false
}

func schedulerLoopConfig() *config.Config {
	return &config.Config{
		RunMode: config.RunModeStandard,
		Gateway: config.GatewayConfig{
			Scheduling: config.GatewaySchedulingConfig{
				OutboxPollIntervalSeconds:     1,
				OutboxPollBatchSize:           1000,
				OutboxDrainMaxBatchesPerTick:  8,
				OutboxDrainMaxDurationSeconds: 8,
				OutboxLagWarnSeconds:          1,
				OutboxLagRebuildSeconds:       1,
				OutboxLagRebuildFailures:      1,
				OutboxBacklogRebuildRows:      1,
				FullRebuildIntervalSeconds:    0,
				DbFallbackEnabled:             true,
				SnapshotMGetChunkSize:         128,
				SnapshotWriteChunkSize:        256,
			},
		},
	}
}

func TestObserveOutboxLagDoesNotTriggerFullRebuild(t *testing.T) {
	cache := &schedulerLoopCache{}
	outbox := &schedulerLoopOutboxRepo{maxID: 100}
	rebuildJobs := &schedulerLoopRebuildJobRepo{}
	svc := NewSchedulerSnapshotService(cache, outbox, nil, nil, schedulerLoopConfig(), rebuildJobs)
	runnerCalls := 0
	svc.fullRebuildRunner = func(ctx context.Context, reason string) error {
		runnerCalls++
		return nil
	}

	svc.observeOutboxLag(context.Background(), SchedulerOutboxEvent{CreatedAt: time.Now().Add(-time.Hour)}, 1)

	require.Equal(t, 0, rebuildJobs.enqueueCount())
	require.Equal(t, 0, runnerCalls)
}

func TestPollOutboxFullRebuildEventEnqueuesDurableJobAndAdvancesWatermark(t *testing.T) {
	cache := &schedulerLoopCache{}
	outbox := &schedulerLoopOutboxRepo{
		batches: [][]SchedulerOutboxEvent{{
			{ID: 42, EventType: SchedulerOutboxEventFullRebuild, CreatedAt: time.Now()},
		}},
		maxID: 42,
	}
	rebuildJobs := &schedulerLoopRebuildJobRepo{}
	svc := NewSchedulerSnapshotService(cache, outbox, nil, nil, schedulerLoopConfig(), rebuildJobs)
	runnerCalls := 0
	svc.fullRebuildRunner = func(ctx context.Context, reason string) error {
		runnerCalls++
		return nil
	}

	svc.pollOutbox()
	svc.wg.Wait()

	require.EqualValues(t, 42, cache.watermark)
	require.Equal(t, []int64{42}, cache.setWatermarks)
	require.Equal(t, 1, rebuildJobs.enqueueCount())
	require.Equal(t, "outbox", rebuildJobs.enqueued[0].reason)
	require.Equal(t, "outbox", rebuildJobs.enqueued[0].source)
	require.NotNil(t, rebuildJobs.enqueued[0].sourceOutbox)
	require.EqualValues(t, 42, *rebuildJobs.enqueued[0].sourceOutbox)
	require.Equal(t, 0, runnerCalls)
}

func TestPollOutboxDoesNotReuseSeenAcrossDrainBatches(t *testing.T) {
	cfg := schedulerLoopConfig()
	cfg.Gateway.Scheduling.OutboxPollBatchSize = 1
	cfg.Gateway.Scheduling.OutboxDrainMaxBatchesPerTick = 2
	cache := &schedulerLoopCache{}
	outbox := &schedulerLoopOutboxRepo{
		batches: [][]SchedulerOutboxEvent{
			{{ID: 1, EventType: SchedulerOutboxEventGroupChanged, GroupID: ptrInt64ForSchedulerLoopTest(7), CreatedAt: time.Now()}},
			{{ID: 2, EventType: SchedulerOutboxEventGroupChanged, GroupID: ptrInt64ForSchedulerLoopTest(7), CreatedAt: time.Now()}},
		},
		maxID: 2,
	}
	accountRepo := &schedulerLoopAccountRepo{accounts: map[int64]*Account{}}
	svc := NewSchedulerSnapshotService(cache, outbox, accountRepo, nil, cfg, &schedulerLoopRebuildJobRepo{})

	svc.pollOutbox()
	svc.wg.Wait()

	require.EqualValues(t, 2, cache.watermark)
	require.Equal(t, 2, countSnapshots(cache.snapshots, SchedulerBucket{GroupID: 7, Platform: PlatformOpenAI, Mode: SchedulerModeSingle}))
}

func TestProcessOutboxBatchCoalescesAccountEvents(t *testing.T) {
	cache := &schedulerLoopCache{}
	accountRepo := &schedulerLoopAccountRepo{accounts: map[int64]*Account{
		1: &Account{ID: 1, Platform: PlatformOpenAI, GroupIDs: []int64{9}},
		2: &Account{ID: 2, Platform: PlatformOpenAI, GroupIDs: []int64{9}},
	}}
	svc := NewSchedulerSnapshotService(cache, nil, accountRepo, nil, schedulerLoopConfig(), &schedulerLoopRebuildJobRepo{})
	events := []SchedulerOutboxEvent{
		{ID: 1, EventType: SchedulerOutboxEventAccountChanged, AccountID: ptrInt64ForSchedulerLoopTest(1)},
		{ID: 2, EventType: SchedulerOutboxEventAccountChanged, AccountID: ptrInt64ForSchedulerLoopTest(2)},
	}

	require.NoError(t, svc.processOutboxBatch(context.Background(), events))

	require.Equal(t, 1, accountRepo.getByIDsCalls)
	require.Equal(t, 1, countSnapshots(cache.snapshots, SchedulerBucket{GroupID: 9, Platform: PlatformOpenAI, Mode: SchedulerModeSingle}))
	require.Equal(t, 1, countSnapshots(cache.snapshots, SchedulerBucket{GroupID: 9, Platform: PlatformOpenAI, Mode: SchedulerModeForced}))
}

func TestIntervalFullRebuildSkippedWhenOutboxBacklogPositive(t *testing.T) {
	cache := &schedulerLoopCache{watermark: 1}
	outbox := &schedulerLoopOutboxRepo{maxID: 2}
	rebuildJobs := &schedulerLoopRebuildJobRepo{}
	svc := NewSchedulerSnapshotService(cache, outbox, nil, nil, schedulerLoopConfig(), rebuildJobs)

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.runFullRebuildWorker(10 * time.Millisecond)
	}()
	time.Sleep(35 * time.Millisecond)
	close(svc.stopCh)
	<-done

	require.Equal(t, 0, rebuildJobs.enqueueCount())
}

func TestFullRebuildJobRetriesWithBackoff(t *testing.T) {
	cache := &schedulerLoopCache{
		fullRebuildLockOK: true,
		listBuckets:       []SchedulerBucket{{GroupID: 0, Platform: PlatformOpenAI, Mode: SchedulerModeSingle}},
	}
	rebuildJobs := &schedulerLoopRebuildJobRepo{
		claims: []*SchedulerRebuildJob{{ID: 10, Reason: "interval", Attempts: 1}},
	}
	svc := NewSchedulerSnapshotService(cache, nil, &schedulerLoopAccountRepo{accounts: map[int64]*Account{}}, nil, schedulerLoopConfig(), rebuildJobs)
	svc.fullRebuildRunner = func(ctx context.Context, reason string) error {
		return errors.New("rebuild failed")
	}

	before := time.Now()
	require.True(t, svc.processOneFullRebuildJob(context.Background()))

	require.Len(t, rebuildJobs.failed, 1)
	require.EqualValues(t, 10, rebuildJobs.failed[0].id)
	require.True(t, rebuildJobs.failed[0].runAfter.After(before.Add(25*time.Second)))
	require.True(t, rebuildJobs.failed[0].runAfter.Before(before.Add(35*time.Second)))
	require.Empty(t, rebuildJobs.succeeded)
}

func TestFullRebuildWorkerUsesGlobalLock(t *testing.T) {
	cache := &schedulerLoopCache{fullRebuildLockOK: false}
	rebuildJobs := &schedulerLoopRebuildJobRepo{
		claims: []*SchedulerRebuildJob{{ID: 11, Reason: "startup", Attempts: 1}},
	}
	svc := NewSchedulerSnapshotService(cache, nil, nil, nil, schedulerLoopConfig(), rebuildJobs)
	runnerCalls := 0
	svc.fullRebuildRunner = func(ctx context.Context, reason string) error {
		runnerCalls++
		return nil
	}

	require.True(t, svc.processOneFullRebuildJob(context.Background()))

	require.Equal(t, 1, cache.fullLockAttempts)
	require.Equal(t, 0, runnerCalls)
	require.Len(t, rebuildJobs.failed, 1)
	require.Empty(t, rebuildJobs.succeeded)
}

func TestPollOutboxBacklogWarningDoesNotRunFullRebuild(t *testing.T) {
	cache := &schedulerLoopCache{}
	outbox := &schedulerLoopOutboxRepo{
		batches: [][]SchedulerOutboxEvent{{
			{
				ID:        1,
				EventType: SchedulerOutboxEventAccountLastUsed,
				Payload: map[string]any{
					"last_used": map[string]any{"101": float64(123)},
				},
				CreatedAt: time.Now().Add(-time.Hour),
			},
		}},
		maxID: 100,
	}
	rebuildJobs := &schedulerLoopRebuildJobRepo{}
	svc := NewSchedulerSnapshotService(cache, outbox, nil, nil, schedulerLoopConfig(), rebuildJobs)
	runnerCalls := 0
	svc.fullRebuildRunner = func(ctx context.Context, reason string) error {
		runnerCalls++
		return nil
	}

	svc.pollOutbox()
	svc.wg.Wait()

	require.EqualValues(t, 1, cache.watermark)
	require.Equal(t, 0, rebuildJobs.enqueueCount())
	require.Equal(t, 0, runnerCalls)
}

func countSnapshots(snapshots []SchedulerBucket, target SchedulerBucket) int {
	count := 0
	for _, bucket := range snapshots {
		if bucket == target {
			count++
		}
	}
	return count
}

func ptrInt64ForSchedulerLoopTest(v int64) *int64 {
	return &v
}
