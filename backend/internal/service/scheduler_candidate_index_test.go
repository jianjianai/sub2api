package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type schedulerCandidateIndexCacheStub struct {
	SchedulerCache
	candidateAccounts []*Account
	candidateHit      bool
	candidateLimit    int
	snapshotAccounts  []*Account
	getSnapshotCalls  int
	setCandidateCalls int
}

func (c *schedulerCandidateIndexCacheStub) ListCandidateAccounts(ctx context.Context, bucket SchedulerBucket, opts SchedulerCandidateListOptions) ([]*Account, bool, error) {
	c.candidateLimit = opts.Limit
	return c.candidateAccounts, c.candidateHit, nil
}

func (c *schedulerCandidateIndexCacheStub) GetSnapshot(ctx context.Context, bucket SchedulerBucket) ([]*Account, bool, error) {
	c.getSnapshotCalls++
	if len(c.snapshotAccounts) == 0 {
		return nil, false, nil
	}
	return c.snapshotAccounts, true, nil
}

func (c *schedulerCandidateIndexCacheStub) SetCandidateIndex(ctx context.Context, bucket SchedulerBucket, accounts []Account) error {
	c.setCandidateCalls++
	c.candidateHit = true
	c.candidateAccounts = make([]*Account, 0, len(accounts))
	for i := range accounts {
		account := accounts[i]
		c.candidateAccounts = append(c.candidateAccounts, &account)
	}
	return nil
}

func (c *schedulerCandidateIndexCacheStub) TryLockBucket(ctx context.Context, bucket SchedulerBucket, ttl time.Duration) (bool, error) {
	return true, nil
}

func (c *schedulerCandidateIndexCacheStub) UnlockBucket(ctx context.Context, bucket SchedulerBucket) error {
	return nil
}

func TestSchedulerSnapshotService_ListSchedulableAccounts_UsesCandidateIndexWhenEnabled(t *testing.T) {
	cache := &schedulerCandidateIndexCacheStub{
		candidateHit: true,
		candidateAccounts: []*Account{
			{ID: 101, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true},
		},
		snapshotAccounts: []*Account{
			{ID: 202, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true},
		},
	}
	svc := &SchedulerSnapshotService{
		cache:                  cache,
		candidateTargetEnabled: true,
		candidateStatus:        SchedulerCandidateStatusActive,
		cfg: &config.Config{Gateway: config.GatewayConfig{Scheduling: config.GatewaySchedulingConfig{
			CandidateIndexEnabled: true,
			CandidateFetchLimit:   16,
		}}},
	}

	accounts, _, err := svc.ListSchedulableAccounts(context.Background(), nil, PlatformOpenAI, false)
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	require.Equal(t, int64(101), accounts[0].ID)
	require.Equal(t, 16, cache.candidateLimit)
	require.Zero(t, cache.getSnapshotCalls)
}

func TestSchedulerSnapshotService_ListSchedulableAccounts_ReturnsNotReadyOnCandidateMiss(t *testing.T) {
	cache := &schedulerCandidateIndexCacheStub{
		candidateHit: false,
		snapshotAccounts: []*Account{
			{ID: 202, Platform: PlatformOpenAI, Status: StatusActive, Schedulable: true},
		},
	}
	svc := &SchedulerSnapshotService{
		cache:                  cache,
		candidateTargetEnabled: true,
		candidateStatus:        SchedulerCandidateStatusActive,
		cfg: &config.Config{Gateway: config.GatewayConfig{Scheduling: config.GatewaySchedulingConfig{
			CandidateIndexEnabled: true,
			CandidateFetchLimit:   16,
		}}},
	}

	accounts, _, err := svc.ListSchedulableAccounts(context.Background(), nil, PlatformOpenAI, false)
	require.ErrorIs(t, err, ErrSchedulerCacheNotReady)
	require.Nil(t, accounts)
	require.Zero(t, cache.getSnapshotCalls)
}
