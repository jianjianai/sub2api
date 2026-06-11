package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/stretchr/testify/require"
)

type usageRepoStub struct {
	UsageLogRepository
	stats            *usagestats.DashboardStats
	rangeStats       *usagestats.DashboardStats
	groupUsage       []usagestats.GroupUsageSummary
	err              error
	rangeErr         error
	groupUsageErr    error
	calls            int32
	rangeCalls       int32
	groupUsageCalls  int32
	rangeStart       time.Time
	rangeEnd         time.Time
	lastTodayStart   time.Time
	onCall           chan struct{}
	onGroupUsageCall chan struct{}
}

func (s *usageRepoStub) GetDashboardStats(ctx context.Context) (*usagestats.DashboardStats, error) {
	atomic.AddInt32(&s.calls, 1)
	if s.onCall != nil {
		select {
		case s.onCall <- struct{}{}:
		default:
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.stats, nil
}

func (s *usageRepoStub) GetDashboardStatsWithRange(ctx context.Context, start, end time.Time) (*usagestats.DashboardStats, error) {
	atomic.AddInt32(&s.rangeCalls, 1)
	s.rangeStart = start
	s.rangeEnd = end
	if s.rangeErr != nil {
		return nil, s.rangeErr
	}
	if s.rangeStats != nil {
		return s.rangeStats, nil
	}
	return s.stats, nil
}

func (s *usageRepoStub) GetAllGroupUsageSummary(ctx context.Context, todayStart time.Time) ([]usagestats.GroupUsageSummary, error) {
	atomic.AddInt32(&s.groupUsageCalls, 1)
	s.lastTodayStart = todayStart
	if s.onGroupUsageCall != nil {
		select {
		case s.onGroupUsageCall <- struct{}{}:
		default:
		}
	}
	if s.groupUsageErr != nil {
		return nil, s.groupUsageErr
	}
	return s.groupUsage, nil
}

type groupUsageSummaryBlockingRepo struct {
	UsageLogRepository
	base    *usageRepoStub
	release <-chan struct{}
}

func (r groupUsageSummaryBlockingRepo) GetAllGroupUsageSummary(ctx context.Context, todayStart time.Time) ([]usagestats.GroupUsageSummary, error) {
	atomic.AddInt32(&r.base.groupUsageCalls, 1)
	r.base.lastTodayStart = todayStart
	if r.base.onGroupUsageCall != nil {
		select {
		case r.base.onGroupUsageCall <- struct{}{}:
		default:
		}
	}
	select {
	case <-r.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if r.base.groupUsageErr != nil {
		return nil, r.base.groupUsageErr
	}
	return r.base.groupUsage, nil
}

type dashboardCacheStub struct {
	get       func(ctx context.Context) (string, error)
	set       func(ctx context.Context, data string, ttl time.Duration) error
	del       func(ctx context.Context) error
	getCalls  int32
	setCalls  int32
	delCalls  int32
	lastSetMu sync.Mutex
	lastSet   string
}

func (c *dashboardCacheStub) GetDashboardStats(ctx context.Context) (string, error) {
	atomic.AddInt32(&c.getCalls, 1)
	if c.get != nil {
		return c.get(ctx)
	}
	return "", ErrDashboardStatsCacheMiss
}

func (c *dashboardCacheStub) SetDashboardStats(ctx context.Context, data string, ttl time.Duration) error {
	atomic.AddInt32(&c.setCalls, 1)
	c.lastSetMu.Lock()
	c.lastSet = data
	c.lastSetMu.Unlock()
	if c.set != nil {
		return c.set(ctx, data, ttl)
	}
	return nil
}

func (c *dashboardCacheStub) DeleteDashboardStats(ctx context.Context) error {
	atomic.AddInt32(&c.delCalls, 1)
	if c.del != nil {
		return c.del(ctx)
	}
	return nil
}

type dashboardAggregationRepoStub struct {
	watermark time.Time
	err       error
}

func (s *dashboardAggregationRepoStub) AggregateRange(ctx context.Context, start, end time.Time) error {
	return nil
}

func (s *dashboardAggregationRepoStub) RecomputeRange(ctx context.Context, start, end time.Time) error {
	return nil
}

func (s *dashboardAggregationRepoStub) GetAggregationWatermark(ctx context.Context) (time.Time, error) {
	if s.err != nil {
		return time.Time{}, s.err
	}
	return s.watermark, nil
}

func (s *dashboardAggregationRepoStub) UpdateAggregationWatermark(ctx context.Context, aggregatedAt time.Time) error {
	return nil
}

func (s *dashboardAggregationRepoStub) CleanupAggregates(ctx context.Context, hourlyCutoff, dailyCutoff time.Time) error {
	return nil
}

func (s *dashboardAggregationRepoStub) CleanupUsageLogs(ctx context.Context, cutoff time.Time) error {
	return nil
}

func (s *dashboardAggregationRepoStub) CleanupUsageBillingDedup(ctx context.Context, cutoff time.Time) error {
	return nil
}

func (s *dashboardAggregationRepoStub) EnsureUsageLogsPartitions(ctx context.Context, now time.Time) error {
	return nil
}

func (c *dashboardCacheStub) readLastEntry(t *testing.T) dashboardStatsCacheEntry {
	t.Helper()
	c.lastSetMu.Lock()
	data := c.lastSet
	c.lastSetMu.Unlock()

	var entry dashboardStatsCacheEntry
	err := json.Unmarshal([]byte(data), &entry)
	require.NoError(t, err)
	return entry
}

func TestDashboardService_CacheHitFresh(t *testing.T) {
	stats := &usagestats.DashboardStats{
		TotalUsers:     10,
		StatsUpdatedAt: time.Unix(0, 0).UTC().Format(time.RFC3339),
		StatsStale:     true,
	}
	entry := dashboardStatsCacheEntry{
		Stats:     stats,
		UpdatedAt: time.Now().Unix(),
	}
	payload, err := json.Marshal(entry)
	require.NoError(t, err)

	cache := &dashboardCacheStub{
		get: func(ctx context.Context) (string, error) {
			return string(payload), nil
		},
	}
	repo := &usageRepoStub{
		stats: &usagestats.DashboardStats{TotalUsers: 99},
	}
	aggRepo := &dashboardAggregationRepoStub{watermark: time.Unix(0, 0).UTC()}
	cfg := &config.Config{
		Dashboard: config.DashboardCacheConfig{Enabled: true},
		DashboardAgg: config.DashboardAggregationConfig{
			Enabled: true,
		},
	}
	svc := NewDashboardService(repo, aggRepo, cache, cfg)

	got, err := svc.GetDashboardStats(context.Background())
	require.NoError(t, err)
	require.Equal(t, stats, got)
	require.Equal(t, int32(0), atomic.LoadInt32(&repo.calls))
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.getCalls))
	require.Equal(t, int32(0), atomic.LoadInt32(&cache.setCalls))
}

func TestDashboardService_CacheMiss_StoresCache(t *testing.T) {
	stats := &usagestats.DashboardStats{
		TotalUsers:     7,
		StatsUpdatedAt: time.Unix(0, 0).UTC().Format(time.RFC3339),
		StatsStale:     true,
	}
	cache := &dashboardCacheStub{
		get: func(ctx context.Context) (string, error) {
			return "", ErrDashboardStatsCacheMiss
		},
	}
	repo := &usageRepoStub{stats: stats}
	aggRepo := &dashboardAggregationRepoStub{watermark: time.Unix(0, 0).UTC()}
	cfg := &config.Config{
		Dashboard: config.DashboardCacheConfig{Enabled: true},
		DashboardAgg: config.DashboardAggregationConfig{
			Enabled: true,
		},
	}
	svc := NewDashboardService(repo, aggRepo, cache, cfg)

	got, err := svc.GetDashboardStats(context.Background())
	require.NoError(t, err)
	require.Equal(t, stats, got)
	require.Equal(t, int32(1), atomic.LoadInt32(&repo.calls))
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.getCalls))
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.setCalls))
	entry := cache.readLastEntry(t)
	require.Equal(t, stats, entry.Stats)
	require.WithinDuration(t, time.Now(), time.Unix(entry.UpdatedAt, 0), time.Second)
}

func TestDashboardService_CacheDisabled_SkipsCache(t *testing.T) {
	stats := &usagestats.DashboardStats{
		TotalUsers:     3,
		StatsUpdatedAt: time.Unix(0, 0).UTC().Format(time.RFC3339),
		StatsStale:     true,
	}
	cache := &dashboardCacheStub{
		get: func(ctx context.Context) (string, error) {
			return "", nil
		},
	}
	repo := &usageRepoStub{stats: stats}
	aggRepo := &dashboardAggregationRepoStub{watermark: time.Unix(0, 0).UTC()}
	cfg := &config.Config{
		Dashboard: config.DashboardCacheConfig{Enabled: false},
		DashboardAgg: config.DashboardAggregationConfig{
			Enabled: true,
		},
	}
	svc := NewDashboardService(repo, aggRepo, cache, cfg)

	got, err := svc.GetDashboardStats(context.Background())
	require.NoError(t, err)
	require.Equal(t, stats, got)
	require.Equal(t, int32(1), atomic.LoadInt32(&repo.calls))
	require.Equal(t, int32(0), atomic.LoadInt32(&cache.getCalls))
	require.Equal(t, int32(0), atomic.LoadInt32(&cache.setCalls))
}

func TestDashboardService_CacheHitStale_TriggersAsyncRefresh(t *testing.T) {
	staleStats := &usagestats.DashboardStats{
		TotalUsers:     11,
		StatsUpdatedAt: time.Unix(0, 0).UTC().Format(time.RFC3339),
		StatsStale:     true,
	}
	entry := dashboardStatsCacheEntry{
		Stats:     staleStats,
		UpdatedAt: time.Now().Add(-defaultDashboardStatsFreshTTL * 2).Unix(),
	}
	payload, err := json.Marshal(entry)
	require.NoError(t, err)

	cache := &dashboardCacheStub{
		get: func(ctx context.Context) (string, error) {
			return string(payload), nil
		},
	}
	refreshCh := make(chan struct{}, 1)
	repo := &usageRepoStub{
		stats:  &usagestats.DashboardStats{TotalUsers: 22},
		onCall: refreshCh,
	}
	aggRepo := &dashboardAggregationRepoStub{watermark: time.Unix(0, 0).UTC()}
	cfg := &config.Config{
		Dashboard: config.DashboardCacheConfig{Enabled: true},
		DashboardAgg: config.DashboardAggregationConfig{
			Enabled: true,
		},
	}
	svc := NewDashboardService(repo, aggRepo, cache, cfg)

	got, err := svc.GetDashboardStats(context.Background())
	require.NoError(t, err)
	require.Equal(t, staleStats, got)

	select {
	case <-refreshCh:
	case <-time.After(1 * time.Second):
		t.Fatal("等待异步刷新超时")
	}
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&cache.setCalls) >= 1
	}, 1*time.Second, 10*time.Millisecond)
}

func TestDashboardService_CacheParseError_EvictsAndRefetches(t *testing.T) {
	cache := &dashboardCacheStub{
		get: func(ctx context.Context) (string, error) {
			return "not-json", nil
		},
	}
	stats := &usagestats.DashboardStats{TotalUsers: 9}
	repo := &usageRepoStub{stats: stats}
	aggRepo := &dashboardAggregationRepoStub{watermark: time.Unix(0, 0).UTC()}
	cfg := &config.Config{
		Dashboard: config.DashboardCacheConfig{Enabled: true},
		DashboardAgg: config.DashboardAggregationConfig{
			Enabled: true,
		},
	}
	svc := NewDashboardService(repo, aggRepo, cache, cfg)

	got, err := svc.GetDashboardStats(context.Background())
	require.NoError(t, err)
	require.Equal(t, stats, got)
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.delCalls))
	require.Equal(t, int32(1), atomic.LoadInt32(&repo.calls))
}

func TestDashboardService_CacheParseError_RepoFailure(t *testing.T) {
	cache := &dashboardCacheStub{
		get: func(ctx context.Context) (string, error) {
			return "not-json", nil
		},
	}
	repo := &usageRepoStub{err: errors.New("db down")}
	aggRepo := &dashboardAggregationRepoStub{watermark: time.Unix(0, 0).UTC()}
	cfg := &config.Config{
		Dashboard: config.DashboardCacheConfig{Enabled: true},
		DashboardAgg: config.DashboardAggregationConfig{
			Enabled: true,
		},
	}
	svc := NewDashboardService(repo, aggRepo, cache, cfg)

	_, err := svc.GetDashboardStats(context.Background())
	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.delCalls))
}

func TestDashboardService_StatsUpdatedAtEpochWhenMissing(t *testing.T) {
	stats := &usagestats.DashboardStats{}
	repo := &usageRepoStub{stats: stats}
	aggRepo := &dashboardAggregationRepoStub{watermark: time.Unix(0, 0).UTC()}
	cfg := &config.Config{Dashboard: config.DashboardCacheConfig{Enabled: false}}
	svc := NewDashboardService(repo, aggRepo, nil, cfg)

	got, err := svc.GetDashboardStats(context.Background())
	require.NoError(t, err)
	require.Equal(t, "1970-01-01T00:00:00Z", got.StatsUpdatedAt)
	require.True(t, got.StatsStale)
}

func TestDashboardService_StatsStaleFalseWhenFresh(t *testing.T) {
	aggNow := time.Now().UTC().Truncate(time.Second)
	stats := &usagestats.DashboardStats{}
	repo := &usageRepoStub{stats: stats}
	aggRepo := &dashboardAggregationRepoStub{watermark: aggNow}
	cfg := &config.Config{
		Dashboard: config.DashboardCacheConfig{Enabled: false},
		DashboardAgg: config.DashboardAggregationConfig{
			Enabled:         true,
			IntervalSeconds: 60,
			LookbackSeconds: 120,
		},
	}
	svc := NewDashboardService(repo, aggRepo, nil, cfg)

	got, err := svc.GetDashboardStats(context.Background())
	require.NoError(t, err)
	require.Equal(t, aggNow.Format(time.RFC3339), got.StatsUpdatedAt)
	require.False(t, got.StatsStale)
}

func TestDashboardService_AggDisabled_UsesUsageLogsFallback(t *testing.T) {
	expected := &usagestats.DashboardStats{TotalUsers: 42}
	repo := &usageRepoStub{
		rangeStats: expected,
		err:        errors.New("should not call aggregated stats"),
	}
	cfg := &config.Config{
		Dashboard: config.DashboardCacheConfig{Enabled: false},
		DashboardAgg: config.DashboardAggregationConfig{
			Enabled: false,
			Retention: config.DashboardAggregationRetentionConfig{
				UsageLogsDays: 7,
			},
		},
	}
	svc := NewDashboardService(repo, nil, nil, cfg)

	got, err := svc.GetDashboardStats(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(42), got.TotalUsers)
	require.Equal(t, int32(0), atomic.LoadInt32(&repo.calls))
	require.Equal(t, int32(1), atomic.LoadInt32(&repo.rangeCalls))
	require.False(t, repo.rangeEnd.IsZero())
	require.Equal(t, truncateToDayUTC(repo.rangeEnd.AddDate(0, 0, -7)), repo.rangeStart)
}

func TestDashboardService_GroupUsageSummaryCachesSameTodayStart(t *testing.T) {
	todayStart := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
	expected := []usagestats.GroupUsageSummary{
		{GroupID: 10, TotalCost: 12.5, TodayCost: 1.25},
	}
	repo := &usageRepoStub{groupUsage: expected}
	svc := NewDashboardService(repo, nil, nil, nil)

	first, err := svc.GetGroupUsageSummary(context.Background(), todayStart)
	require.NoError(t, err)
	second, err := svc.GetGroupUsageSummary(context.Background(), todayStart)
	require.NoError(t, err)

	require.Equal(t, expected, first)
	require.Equal(t, expected, second)
	require.Equal(t, int32(1), atomic.LoadInt32(&repo.groupUsageCalls))
}

func TestDashboardService_GroupUsageSummaryCollapsesBurst(t *testing.T) {
	todayStart := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
	repo := &usageRepoStub{
		groupUsage: []usagestats.GroupUsageSummary{{GroupID: 10, TotalCost: 12.5, TodayCost: 1.25}},
	}
	svc := NewDashboardService(repo, nil, nil, nil)

	for i := 0; i < 100; i++ {
		results, err := svc.GetGroupUsageSummary(context.Background(), todayStart)
		require.NoError(t, err)
		require.Len(t, results, 1)
	}

	require.Equal(t, int32(1), atomic.LoadInt32(&repo.groupUsageCalls))
}

func TestDashboardService_GroupUsageSummaryCacheSeparatesTodayStart(t *testing.T) {
	firstDay := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
	secondDay := firstDay.Add(24 * time.Hour)
	repo := &usageRepoStub{
		groupUsage: []usagestats.GroupUsageSummary{{GroupID: 10, TotalCost: 12.5, TodayCost: 1.25}},
	}
	svc := NewDashboardService(repo, nil, nil, nil)

	_, err := svc.GetGroupUsageSummary(context.Background(), firstDay)
	require.NoError(t, err)
	_, err = svc.GetGroupUsageSummary(context.Background(), secondDay)
	require.NoError(t, err)

	require.Equal(t, int32(2), atomic.LoadInt32(&repo.groupUsageCalls))
	require.Equal(t, secondDay, repo.lastTodayStart)
}

func TestDashboardService_GroupUsageSummarySingleflightCollapsesConcurrentLoads(t *testing.T) {
	todayStart := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	blockingRepo := &usageRepoStub{
		groupUsage:       []usagestats.GroupUsageSummary{{GroupID: 10, TotalCost: 12.5, TodayCost: 1.25}},
		onGroupUsageCall: started,
	}
	svc := NewDashboardService(groupUsageSummaryBlockingRepo{
		base:    blockingRepo,
		release: release,
	}, nil, nil, nil)

	errs := make(chan error, 2)
	go func() {
		_, err := svc.GetGroupUsageSummary(context.Background(), todayStart)
		errs <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("等待首次回源超时")
	}

	go func() {
		_, err := svc.GetGroupUsageSummary(context.Background(), todayStart)
		errs <- err
	}()
	time.Sleep(10 * time.Millisecond)

	close(release)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	require.Equal(t, int32(1), atomic.LoadInt32(&blockingRepo.groupUsageCalls))
}
