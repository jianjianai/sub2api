package repository

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newSchedulerCandidateIndexReadModelTest(t *testing.T) (context.Context, *redis.Client, service.SchedulerCandidateIndexReadModel, service.SchedulerBucket) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = rdb.Close()
	})
	bucket := service.SchedulerBucket{GroupID: 7, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	return context.Background(), rdb, NewSchedulerCandidateIndexReadModel(rdb), bucket
}

func TestSchedulerCandidateIndexReadModelSetAndGetPages(t *testing.T) {
	ctx, _, model, bucket := newSchedulerCandidateIndexReadModelTest(t)
	now := time.Now().UTC()
	version, err := model.NextVersion(ctx, bucket)
	require.NoError(t, err)

	accounts := []service.Account{
		{ID: 3, Platform: service.PlatformOpenAI, Status: service.StatusActive, Schedulable: true, Priority: 2},
		{ID: 1, Platform: service.PlatformOpenAI, Status: service.StatusActive, Schedulable: true, Priority: 0},
		{ID: 2, Platform: service.PlatformOpenAI, Status: service.StatusActive, Schedulable: true, Priority: 1},
	}
	require.NoError(t, model.SetBucketCandidates(ctx, bucket, accounts, version, now))

	first, err := model.GetCandidatePage(ctx, bucket, 0, 2)
	require.NoError(t, err)
	require.True(t, first.Hit)
	require.True(t, first.HasMore)
	require.Equal(t, []int64{1, 2}, []int64{first.Accounts[0].ID, first.Accounts[1].ID})

	second, err := model.GetCandidatePage(ctx, bucket, 2, 2)
	require.NoError(t, err)
	require.True(t, second.Hit)
	require.False(t, second.HasMore)
	require.Equal(t, int64(3), second.Accounts[0].ID)
}

func TestSchedulerCandidateIndexReadModelSkipsBadJSON(t *testing.T) {
	ctx, rdb, model, bucket := newSchedulerCandidateIndexReadModelTest(t)
	version, err := model.NextVersion(ctx, bucket)
	require.NoError(t, err)
	require.NoError(t, model.SetBucketCandidates(ctx, bucket, []service.Account{
		{ID: 1, Platform: service.PlatformOpenAI, Status: service.StatusActive, Schedulable: true},
		{ID: 2, Platform: service.PlatformOpenAI, Status: service.StatusActive, Schedulable: true},
	}, version, time.Now().UTC()))
	require.NoError(t, rdb.HSet(ctx, schedulerCandidateAccountsKey(bucket, version), "1", "{bad-json").Err())

	page, err := model.GetCandidatePage(ctx, bucket, 0, 2)
	require.NoError(t, err)
	require.True(t, page.Hit)
	require.Len(t, page.Accounts, 1)
	require.Equal(t, int64(2), page.Accounts[0].ID)
}

func TestSchedulerCandidateIndexReadModelDeleteBucket(t *testing.T) {
	ctx, _, model, bucket := newSchedulerCandidateIndexReadModelTest(t)
	version, err := model.NextVersion(ctx, bucket)
	require.NoError(t, err)
	require.NoError(t, model.SetBucketCandidates(ctx, bucket, []service.Account{{ID: 1}}, version, time.Now().UTC()))

	require.NoError(t, model.DeleteBucket(ctx, bucket))
	page, err := model.GetCandidatePage(ctx, bucket, 0, 10)
	require.NoError(t, err)
	require.False(t, page.Hit)
}

func TestSchedulerCandidateIndexReadModelChunksAndNoSecretCredentials(t *testing.T) {
	ctx, _, model, bucket := newSchedulerCandidateIndexReadModelTest(t)
	version, err := model.NextVersion(ctx, bucket)
	require.NoError(t, err)
	accounts := make([]service.Account, 0, schedulerCandidateIndexWriteChunkSize+1)
	for i := 0; i <= schedulerCandidateIndexWriteChunkSize; i++ {
		accounts = append(accounts, service.Account{
			ID:       int64(10_000 + i),
			Platform: service.PlatformOpenAI,
			Credentials: map[string]any{
				"api_key":       "secret",
				"access_token":  "token",
				"model_mapping": map[string]any{"gpt-5": "gpt-5"},
			},
		})
	}
	require.NoError(t, model.SetBucketCandidates(ctx, bucket, accounts, version, time.Now().UTC()))

	page, err := model.GetCandidatePage(ctx, bucket, schedulerCandidateIndexWriteChunkSize, 2)
	require.NoError(t, err)
	require.True(t, page.Hit)
	require.Len(t, page.Accounts, 1)
	require.NotContains(t, page.Accounts[0].Credentials, "api_key")
	require.NotContains(t, page.Accounts[0].Credentials, "access_token")
	require.Contains(t, page.Accounts[0].Credentials, "model_mapping")
}

func TestSchedulerCandidateIndexReadModelLowVersionDoesNotRollback(t *testing.T) {
	ctx, rdb, model, bucket := newSchedulerCandidateIndexReadModelTest(t)
	require.NoError(t, model.SetBucketCandidates(ctx, bucket, []service.Account{{ID: 1}}, 2, time.Now().UTC()))
	require.NoError(t, model.SetBucketCandidates(ctx, bucket, []service.Account{{ID: 9}}, 1, time.Now().UTC()))

	active, err := rdb.Get(ctx, schedulerCandidateActiveKey(bucket)).Result()
	require.NoError(t, err)
	require.Equal(t, "2", active)
	page, err := model.GetCandidatePage(ctx, bucket, 0, 10)
	require.NoError(t, err)
	require.Equal(t, int64(1), page.Accounts[0].ID)
	ttl, err := rdb.TTL(ctx, schedulerCandidateAccountsKey(bucket, 1)).Result()
	require.NoError(t, err)
	require.Positive(t, int(ttl.Seconds()))
	require.Equal(t, "2", strconv.FormatInt(page.Meta.Version, 10))
}
