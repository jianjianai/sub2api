//go:build integration

package repository

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestSchedulerCacheSnapshotUsesSlimMetadataButKeepsFullAccount(t *testing.T) {
	ctx := context.Background()
	rdb := testRedis(t)
	cache := NewSchedulerCache(rdb)

	bucket := service.SchedulerBucket{GroupID: 2, Platform: service.PlatformGemini, Mode: service.SchedulerModeSingle}
	now := time.Now().UTC().Truncate(time.Second)
	limitReset := now.Add(10 * time.Minute)
	overloadUntil := now.Add(2 * time.Minute)
	tempUnschedUntil := now.Add(3 * time.Minute)
	windowEnd := now.Add(5 * time.Hour)

	account := service.Account{
		ID:          101,
		Name:        "gemini-heavy",
		Platform:    service.PlatformGemini,
		Type:        service.AccountTypeOAuth,
		Status:      service.StatusActive,
		Schedulable: true,
		Concurrency: 3,
		Priority:    7,
		LastUsedAt:  &now,
		Credentials: map[string]any{
			"api_key":       "gemini-api-key",
			"access_token":  "secret-access-token",
			"project_id":    "proj-1",
			"oauth_type":    "ai_studio",
			"model_mapping": map[string]any{"gemini-2.5-pro": "gemini-2.5-pro"},
			"huge_blob":     strings.Repeat("x", 4096),
		},
		Extra: map[string]any{
			"mixed_scheduling":             true,
			"window_cost_limit":            12.5,
			"window_cost_sticky_reserve":   8.0,
			"max_sessions":                 4,
			"session_idle_timeout_minutes": 11,
			"unused_large_field":           strings.Repeat("y", 4096),
		},
		RateLimitResetAt:       &limitReset,
		OverloadUntil:          &overloadUntil,
		TempUnschedulableUntil: &tempUnschedUntil,
		SessionWindowStart:     &now,
		SessionWindowEnd:       &windowEnd,
		SessionWindowStatus:    "active",
		GroupIDs:               []int64{bucket.GroupID},
		AccountGroups: []service.AccountGroup{
			{
				AccountID: 101,
				GroupID:   bucket.GroupID,
				Priority:  5,
				Group:     &service.Group{ID: bucket.GroupID, Name: "gemini-group"},
			},
		},
	}

	require.NoError(t, cache.SetSnapshot(ctx, bucket, []service.Account{account}))

	snapshot, hit, err := cache.GetSnapshot(ctx, bucket)
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, snapshot, 1)

	got := snapshot[0]
	require.NotNil(t, got)
	require.Equal(t, "gemini-api-key", got.GetCredential("api_key"))
	require.Equal(t, "proj-1", got.GetCredential("project_id"))
	require.Equal(t, "ai_studio", got.GetCredential("oauth_type"))
	require.NotEmpty(t, got.GetModelMapping())
	require.Empty(t, got.GetCredential("access_token"))
	require.Empty(t, got.GetCredential("huge_blob"))
	require.Equal(t, true, got.Extra["mixed_scheduling"])
	require.Equal(t, 12.5, got.GetWindowCostLimit())
	require.Equal(t, 8.0, got.GetWindowCostStickyReserve())
	require.Equal(t, 4, got.GetMaxSessions())
	require.Equal(t, 11, got.GetSessionIdleTimeoutMinutes())
	require.Nil(t, got.Extra["unused_large_field"])
	require.Equal(t, []int64{bucket.GroupID}, got.GroupIDs)
	require.Len(t, got.AccountGroups, 1)
	require.Equal(t, account.ID, got.AccountGroups[0].AccountID)
	require.Equal(t, bucket.GroupID, got.AccountGroups[0].GroupID)
	require.Nil(t, got.AccountGroups[0].Group)

	full, err := cache.GetAccount(ctx, account.ID)
	require.NoError(t, err)
	require.NotNil(t, full)
	require.Equal(t, "secret-access-token", full.GetCredential("access_token"))
	require.Equal(t, strings.Repeat("x", 4096), full.GetCredential("huge_blob"))
	require.Len(t, full.AccountGroups, 1)
	require.NotNil(t, full.AccountGroups[0].Group)
}

func TestSchedulerCacheCandidateIndexLifecycle(t *testing.T) {
	ctx := context.Background()
	rdb := testRedis(t)
	cache := NewSchedulerCache(rdb)

	bucket := service.SchedulerBucket{GroupID: 5, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	now := time.Now().UTC().Truncate(time.Second)
	lastUsed := now.Add(-time.Minute)
	blockedUntil := now.Add(10 * time.Minute)
	accounts := []service.Account{
		{
			ID:          201,
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeAPIKey,
			Status:      service.StatusActive,
			Schedulable: true,
			Priority:    3,
			LastUsedAt:  &lastUsed,
			GroupIDs:    []int64{bucket.GroupID},
			AccountGroups: []service.AccountGroup{
				{AccountID: 201, GroupID: bucket.GroupID, Priority: 1},
			},
		},
		{
			ID:          202,
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeAPIKey,
			Status:      service.StatusActive,
			Schedulable: true,
			Priority:    2,
			GroupIDs:    []int64{bucket.GroupID},
		},
		{
			ID:                     203,
			Platform:               service.PlatformOpenAI,
			Type:                   service.AccountTypeAPIKey,
			Status:                 service.StatusActive,
			Schedulable:            true,
			Priority:               0,
			TempUnschedulableUntil: &blockedUntil,
			GroupIDs:               []int64{bucket.GroupID},
		},
	}

	require.NoError(t, cache.SetSnapshot(ctx, bucket, accounts))

	ids, err := rdb.ZRange(ctx, schedulerCandidateKey(bucket), 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"201", "202"}, ids)

	reverse, err := rdb.SMembers(ctx, schedulerAccountBucketsKey("201")).Result()
	require.NoError(t, err)
	require.Contains(t, reverse, bucket.String())
	reverse, err = rdb.SMembers(ctx, schedulerAccountBucketsKey("203")).Result()
	require.NoError(t, err)
	require.Empty(t, reverse)

	candidates, hit, err := cache.ListCandidateAccounts(ctx, bucket, service.SchedulerCandidateListOptions{Limit: 8})
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, candidates, 2)
	require.Equal(t, int64(201), candidates[0].ID)

	removed, err := cache.RemoveAccountFromCandidates(ctx, 201, service.SchedulerBlockedAccountState{
		AccountID: 201,
		Until:     blockedUntil,
		Reason:    "oauth_401",
		Source:    "test",
		UpdatedAt: now,
	})
	require.NoError(t, err)
	require.Equal(t, []service.SchedulerBucket{bucket}, removed)

	ids, err = rdb.ZRange(ctx, schedulerCandidateKey(bucket), 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"202"}, ids)
	blockedIDs, err := cache.PopDueBlockedAccounts(ctx, blockedUntil.Add(time.Second), 10)
	require.NoError(t, err)
	require.Contains(t, blockedIDs, int64(201))

	accounts[0].TempUnschedulableUntil = nil
	require.NoError(t, cache.RestoreAccountCandidates(ctx, &accounts[0], []service.SchedulerBucket{bucket}))
	ids, err = rdb.ZRange(ctx, schedulerCandidateKey(bucket), 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"201", "202"}, ids)
	blockedIDs, err = cache.PopDueBlockedAccounts(ctx, blockedUntil.Add(time.Second), 10)
	require.NoError(t, err)
	require.NotContains(t, blockedIDs, int64(201))
}

func TestSchedulerCacheListCandidateAccountsLimitAndMetaMissing(t *testing.T) {
	ctx := context.Background()
	rdb := testRedis(t)
	cache := NewSchedulerCache(rdb)

	bucket := service.SchedulerBucket{GroupID: 6, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	accounts := make([]service.Account, 0, 10)
	for i := 0; i < 10; i++ {
		accounts = append(accounts, service.Account{
			ID:          int64(300 + i),
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeAPIKey,
			Status:      service.StatusActive,
			Schedulable: true,
			Priority:    i,
			GroupIDs:    []int64{bucket.GroupID},
		})
	}
	require.NoError(t, cache.SetSnapshot(ctx, bucket, accounts))

	candidates, hit, err := cache.ListCandidateAccounts(ctx, bucket, service.SchedulerCandidateListOptions{Limit: 8})
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, candidates, 8)

	require.NoError(t, rdb.Del(ctx, schedulerAccountMetaKey("300")).Err())
	candidates, hit, err = cache.ListCandidateAccounts(ctx, bucket, service.SchedulerCandidateListOptions{Limit: 8})
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, candidates, 7)

	ids, err := rdb.ZRange(ctx, schedulerCandidateKey(bucket), 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, "300")
}

func TestSchedulerCacheListCandidateAccountsEmptyBucketHits(t *testing.T) {
	ctx := context.Background()
	rdb := testRedis(t)
	cache := NewSchedulerCache(rdb)

	bucket := service.SchedulerBucket{GroupID: 7, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	require.NoError(t, cache.SetSnapshot(ctx, bucket, nil))

	candidates, hit, err := cache.ListCandidateAccounts(ctx, bucket, service.SchedulerCandidateListOptions{Limit: 8})
	require.NoError(t, err)
	require.True(t, hit)
	require.Empty(t, candidates)
}

func TestSchedulerCacheListCandidateAccountsMissingNonEmptyIndexMisses(t *testing.T) {
	ctx := context.Background()
	rdb := testRedis(t)
	cache := NewSchedulerCache(rdb)

	bucket := service.SchedulerBucket{GroupID: 7, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeForced}
	account := service.Account{
		ID:          701,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeAPIKey,
		Status:      service.StatusActive,
		Schedulable: true,
		GroupIDs:    []int64{bucket.GroupID},
	}
	require.NoError(t, cache.SetSnapshot(ctx, bucket, []service.Account{account}))
	require.NoError(t, rdb.Del(ctx, schedulerCandidateKey(bucket)).Err())

	candidates, hit, err := cache.ListCandidateAccounts(ctx, bucket, service.SchedulerCandidateListOptions{Limit: 8})
	require.NoError(t, err)
	require.False(t, hit)
	require.Nil(t, candidates)
}

func TestSchedulerCacheListCandidateAccountsAllCandidatesBlockedHitsEmpty(t *testing.T) {
	ctx := context.Background()
	rdb := testRedis(t)
	cache := NewSchedulerCache(rdb)

	bucket := service.SchedulerBucket{GroupID: 7, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	account := service.Account{
		ID:          702,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeAPIKey,
		Status:      service.StatusActive,
		Schedulable: true,
		GroupIDs:    []int64{bucket.GroupID},
	}
	require.NoError(t, cache.SetSnapshot(ctx, bucket, []service.Account{account}))
	_, err := cache.RemoveAccountFromCandidates(ctx, account.ID, service.SchedulerBlockedAccountState{
		AccountID: account.ID,
		Until:     time.Now().Add(time.Minute),
		Reason:    "test_block",
		Source:    "test",
	})
	require.NoError(t, err)

	candidates, hit, err := cache.ListCandidateAccounts(ctx, bucket, service.SchedulerCandidateListOptions{Limit: 8})
	require.NoError(t, err)
	require.True(t, hit)
	require.Empty(t, candidates)
}

func TestSchedulerCacheUpdateCandidateScores(t *testing.T) {
	ctx := context.Background()
	rdb := testRedis(t)
	cache := NewSchedulerCache(rdb)

	bucket := service.SchedulerBucket{GroupID: 8, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	account := service.Account{
		ID:          401,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeAPIKey,
		Status:      service.StatusActive,
		Schedulable: true,
		Priority:    1,
		GroupIDs:    []int64{bucket.GroupID},
	}
	require.NoError(t, cache.SetSnapshot(ctx, bucket, []service.Account{account}))

	usedAt := time.UnixMilli(9999).UTC()
	require.NoError(t, cache.UpdateCandidateScores(ctx, map[int64]time.Time{account.ID: usedAt}))

	score, err := rdb.ZScore(ctx, schedulerCandidateKey(bucket), "401").Result()
	require.NoError(t, err)
	require.Equal(t, float64(1)*10000000000000+float64(9999), score)

	cached, err := cache.GetAccount(ctx, account.ID)
	require.NoError(t, err)
	require.NotNil(t, cached.LastUsedAt)
	require.Equal(t, usedAt, *cached.LastUsedAt)
}
