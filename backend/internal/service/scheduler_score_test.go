package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type schedulerScoreReadModelStub struct {
	nextVersion int64
	scores      map[string]map[int64]SchedulerScoreSnapshot
	setCalls    []schedulerScoreSetCall
	deleted     []SchedulerBucket
	reads       []schedulerScoreReadCall
}

type schedulerScoreSetCall struct {
	bucket  SchedulerBucket
	scores  []SchedulerScoreSnapshot
	version int64
}

type schedulerScoreReadCall struct {
	bucket     SchedulerBucket
	accountIDs []int64
}

type schedulerScoreConcurrencyCacheStub struct {
	activeLoadMap      map[int64]*AccountLoadInfo
	activeLoadMapErr   error
	activeLoadMapCalls atomic.Int64
	loadBatchCalls     atomic.Int64
}

func (m *schedulerScoreReadModelStub) NextVersion(context.Context, SchedulerBucket) (int64, error) {
	if m.nextVersion <= 0 {
		m.nextVersion = 1
	}
	version := m.nextVersion
	m.nextVersion++
	return version, nil
}

func (m *schedulerScoreReadModelStub) GetScores(_ context.Context, bucket SchedulerBucket, accountIDs []int64) (map[int64]SchedulerScoreSnapshot, error) {
	m.reads = append(m.reads, schedulerScoreReadCall{bucket: bucket, accountIDs: append([]int64(nil), accountIDs...)})
	out := make(map[int64]SchedulerScoreSnapshot)
	if m.scores == nil {
		return out, nil
	}
	for _, accountID := range accountIDs {
		if score, ok := m.scores[bucket.String()][accountID]; ok {
			out[accountID] = score
		}
	}
	return out, nil
}

func (m *schedulerScoreReadModelStub) SetBucketScores(_ context.Context, bucket SchedulerBucket, scores []SchedulerScoreSnapshot, version int64, _ time.Time) error {
	m.setCalls = append(m.setCalls, schedulerScoreSetCall{
		bucket:  bucket,
		scores:  append([]SchedulerScoreSnapshot(nil), scores...),
		version: version,
	})
	return nil
}

func (m *schedulerScoreReadModelStub) DeleteBucket(_ context.Context, bucket SchedulerBucket) error {
	m.deleted = append(m.deleted, bucket)
	return nil
}

func (c *schedulerScoreConcurrencyCacheStub) AcquireAccountSlot(context.Context, int64, int, string) (bool, error) {
	return true, nil
}
func (c *schedulerScoreConcurrencyCacheStub) ReleaseAccountSlot(context.Context, int64, string) error {
	return nil
}
func (c *schedulerScoreConcurrencyCacheStub) GetAccountConcurrency(context.Context, int64) (int, error) {
	return 0, nil
}
func (c *schedulerScoreConcurrencyCacheStub) GetAccountConcurrencyBatch(_ context.Context, accountIDs []int64) (map[int64]int, error) {
	out := make(map[int64]int, len(accountIDs))
	for _, accountID := range accountIDs {
		out[accountID] = 0
	}
	return out, nil
}
func (c *schedulerScoreConcurrencyCacheStub) IncrementAccountWaitCount(context.Context, int64, int) (bool, error) {
	return true, nil
}
func (c *schedulerScoreConcurrencyCacheStub) DecrementAccountWaitCount(context.Context, int64) error {
	return nil
}
func (c *schedulerScoreConcurrencyCacheStub) GetAccountWaitingCount(context.Context, int64) (int, error) {
	return 0, nil
}
func (c *schedulerScoreConcurrencyCacheStub) AcquireUserSlot(context.Context, int64, int, string) (bool, error) {
	return true, nil
}
func (c *schedulerScoreConcurrencyCacheStub) ReleaseUserSlot(context.Context, int64, string) error {
	return nil
}
func (c *schedulerScoreConcurrencyCacheStub) GetUserConcurrency(context.Context, int64) (int, error) {
	return 0, nil
}
func (c *schedulerScoreConcurrencyCacheStub) IncrementWaitCount(context.Context, int64, int) (bool, error) {
	return true, nil
}
func (c *schedulerScoreConcurrencyCacheStub) DecrementWaitCount(context.Context, int64) error {
	return nil
}
func (c *schedulerScoreConcurrencyCacheStub) GetAccountsLoadBatch(context.Context, []AccountWithConcurrency) (map[int64]*AccountLoadInfo, error) {
	c.loadBatchCalls.Add(1)
	return map[int64]*AccountLoadInfo{}, nil
}
func (c *schedulerScoreConcurrencyCacheStub) GetActiveAccountLoadMap(context.Context) (map[int64]*AccountLoadInfo, error) {
	c.activeLoadMapCalls.Add(1)
	if c.activeLoadMap != nil {
		return c.activeLoadMap, c.activeLoadMapErr
	}
	return map[int64]*AccountLoadInfo{}, c.activeLoadMapErr
}
func (c *schedulerScoreConcurrencyCacheStub) GetUsersLoadBatch(context.Context, []UserWithConcurrency) (map[int64]*UserLoadInfo, error) {
	return map[int64]*UserLoadInfo{}, nil
}
func (c *schedulerScoreConcurrencyCacheStub) CleanupExpiredAccountSlots(context.Context, int64) error {
	return nil
}
func (c *schedulerScoreConcurrencyCacheStub) CleanupExpiredAccountSlotKeys(context.Context) error {
	return nil
}
func (c *schedulerScoreConcurrencyCacheStub) CleanupStaleProcessSlots(context.Context, string) error {
	return nil
}

func TestSchedulerScoreServiceRebuildUsesActiveLoadMap(t *testing.T) {
	readModel := &schedulerScoreReadModelStub{}
	cache := &schedulerScoreConcurrencyCacheStub{activeLoadMap: map[int64]*AccountLoadInfo{
		1: {AccountID: 1, CurrentConcurrency: 8, WaitingCount: 0, LoadRate: 999},
	}}
	svc := NewSchedulerScoreService(readModel, NewConcurrencyService(cache), nil, nil)
	bucket := SchedulerBucket{GroupID: 0, Platform: PlatformOpenAI, Mode: SchedulerModeSingle}
	accounts := []Account{
		{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 10, Priority: 1},
		{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 10, Priority: 1},
	}

	require.NoError(t, svc.RebuildBucketScores(context.Background(), bucket, accounts))

	require.Equal(t, int64(1), cache.activeLoadMapCalls.Load())
	require.Equal(t, int64(0), cache.loadBatchCalls.Load())
	require.Len(t, readModel.setCalls, 1)
	rows := readModel.setCalls[0].scores
	require.Len(t, rows, 2)
	require.Equal(t, int64(2), rows[0].AccountID, "missing active load should be treated as zero load and rank ahead")
	require.Equal(t, 1, rows[0].Rank)
	require.Equal(t, int64(1), rows[1].AccountID)
	require.Equal(t, 2, rows[1].Rank)
	require.Equal(t, int64(1), rows[0].Version)
}

func TestSchedulerScoreServiceDeletesUnsupportedBuckets(t *testing.T) {
	readModel := &schedulerScoreReadModelStub{}
	svc := NewSchedulerScoreService(readModel, nil, nil, nil)

	require.NoError(t, svc.RebuildBucketScores(context.Background(), SchedulerBucket{GroupID: 0, Platform: PlatformGemini, Mode: SchedulerModeSingle}, []Account{{ID: 1}}))
	require.NoError(t, svc.RebuildBucketScores(context.Background(), SchedulerBucket{GroupID: 0, Platform: PlatformOpenAI, Mode: SchedulerModeForced}, []Account{{ID: 1}}))

	require.Len(t, readModel.deleted, 2)
	require.Empty(t, readModel.setCalls)
}

func TestSchedulerScoreServiceSkipsPublishOnActiveLoadError(t *testing.T) {
	readModel := &schedulerScoreReadModelStub{}
	cache := &schedulerScoreConcurrencyCacheStub{activeLoadMapErr: errors.New("redis down")}
	svc := NewSchedulerScoreService(readModel, NewConcurrencyService(cache), nil, nil)

	err := svc.RebuildBucketScores(context.Background(), SchedulerBucket{GroupID: 0, Platform: PlatformOpenAI, Mode: SchedulerModeSingle}, []Account{
		{ID: 1, Platform: PlatformOpenAI, Concurrency: 1},
	})

	require.Error(t, err)
	require.Empty(t, readModel.setCalls)
}

func TestSchedulerScoreServiceGetAccountListScoresReadsCurrentBuckets(t *testing.T) {
	groupID := int64(9)
	ungrouped := SchedulerBucket{GroupID: 0, Platform: PlatformOpenAI, Mode: SchedulerModeSingle}
	grouped := SchedulerBucket{GroupID: groupID, Platform: PlatformOpenAI, Mode: SchedulerModeSingle}
	readModel := &schedulerScoreReadModelStub{scores: map[string]map[int64]SchedulerScoreSnapshot{
		ungrouped.String(): {
			1: {AccountID: 1, Bucket: ungrouped, GroupID: 0, BaseScore: 1, Rank: 1, BucketSize: 1},
		},
		grouped.String(): {
			2: {AccountID: 2, Bucket: grouped, GroupID: groupID, GroupName: "openai", GroupPriority: ptrServiceInt(3), BaseScore: 2, Rank: 1, BucketSize: 1},
		},
	}}
	svc := NewSchedulerScoreService(readModel, nil, nil, nil)

	baseScores, groupScores := svc.GetAccountListScores(context.Background(), []Account{
		{ID: 1, Platform: PlatformOpenAI},
		{ID: 2, Platform: PlatformOpenAI, AccountGroups: []AccountGroup{{AccountID: 2, GroupID: groupID, Priority: 3, Group: &Group{ID: groupID, Name: "openai"}}}},
		{ID: 3, Platform: PlatformGemini},
	})

	require.Len(t, readModel.reads, 2)
	require.Contains(t, baseScores, int64(1))
	require.Equal(t, 1.0, baseScores[1].BaseScore)
	require.Contains(t, groupScores, int64(2))
	require.Equal(t, "openai", groupScores[2][0].GroupName)
	require.Equal(t, 2.0, baseScores[2].BaseScore)
}

func TestSchedulerScoreServiceSimpleModeReadsUngroupedBucket(t *testing.T) {
	groupID := int64(9)
	ungrouped := SchedulerBucket{GroupID: 0, Platform: PlatformOpenAI, Mode: SchedulerModeSingle}
	readModel := &schedulerScoreReadModelStub{scores: map[string]map[int64]SchedulerScoreSnapshot{
		ungrouped.String(): {
			2: {AccountID: 2, Bucket: ungrouped, GroupID: 0, BaseScore: 2, Rank: 1, BucketSize: 1},
		},
	}}
	svc := NewSchedulerScoreService(readModel, nil, nil, &config.Config{RunMode: config.RunModeSimple})

	baseScores, groupScores := svc.GetAccountListScores(context.Background(), []Account{
		{ID: 2, Platform: PlatformOpenAI, GroupIDs: []int64{groupID}},
	})

	require.Len(t, readModel.reads, 1)
	require.Equal(t, ungrouped, readModel.reads[0].bucket)
	require.Equal(t, 2.0, baseScores[2].BaseScore)
	require.Empty(t, groupScores)
}

func ptrServiceInt(v int) *int {
	return &v
}
