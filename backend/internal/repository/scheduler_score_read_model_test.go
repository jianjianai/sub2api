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

func newSchedulerScoreReadModelTest(t *testing.T) (context.Context, *redis.Client, service.SchedulerScoreReadModel, service.SchedulerBucket) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = rdb.Close()
	})
	bucket := service.SchedulerBucket{GroupID: 7, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	return context.Background(), rdb, NewSchedulerScoreReadModel(rdb), bucket
}

func TestSchedulerScoreReadModelSetAndGetScores(t *testing.T) {
	ctx, _, model, bucket := newSchedulerScoreReadModelTest(t)
	updatedAt := time.Now().UTC()
	version, err := model.NextVersion(ctx, bucket)
	require.NoError(t, err)

	require.NoError(t, model.SetBucketScores(ctx, bucket, []service.SchedulerScoreSnapshot{
		{AccountID: 1, Bucket: bucket, GroupID: bucket.GroupID, BaseScore: 1.1, Version: version, UpdatedAt: updatedAt, BucketSize: 2},
		{AccountID: 2, Bucket: bucket, GroupID: bucket.GroupID, BaseScore: 2.2, Version: version, UpdatedAt: updatedAt, BucketSize: 2},
	}, version, updatedAt))

	got, err := model.GetScores(ctx, bucket, []int64{2, 2, 3})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, 2.2, got[2].BaseScore)
	require.NotContains(t, got, int64(3))
}

func TestSchedulerScoreReadModelDeleteBucket(t *testing.T) {
	ctx, _, model, bucket := newSchedulerScoreReadModelTest(t)
	updatedAt := time.Now().UTC()
	version, err := model.NextVersion(ctx, bucket)
	require.NoError(t, err)
	require.NoError(t, model.SetBucketScores(ctx, bucket, []service.SchedulerScoreSnapshot{
		{AccountID: 1, Bucket: bucket, GroupID: bucket.GroupID, BaseScore: 1.1, Version: version, UpdatedAt: updatedAt, BucketSize: 1},
	}, version, updatedAt))

	require.NoError(t, model.DeleteBucket(ctx, bucket))
	got, err := model.GetScores(ctx, bucket, []int64{1})
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestSchedulerScoreReadModelSkipsBadJSON(t *testing.T) {
	ctx, rdb, model, bucket := newSchedulerScoreReadModelTest(t)
	updatedAt := time.Now().UTC()
	version, err := model.NextVersion(ctx, bucket)
	require.NoError(t, err)
	require.NoError(t, model.SetBucketScores(ctx, bucket, []service.SchedulerScoreSnapshot{
		{AccountID: 1, Bucket: bucket, GroupID: bucket.GroupID, BaseScore: 1.1, Version: version, UpdatedAt: updatedAt, BucketSize: 2},
		{AccountID: 2, Bucket: bucket, GroupID: bucket.GroupID, BaseScore: 2.2, Version: version, UpdatedAt: updatedAt, BucketSize: 2},
	}, version, updatedAt))
	require.NoError(t, rdb.HSet(ctx, schedulerScoreScoresKey(bucket, strconv.FormatInt(version, 10)), "1", "{bad-json").Err())

	got, err := model.GetScores(ctx, bucket, []int64{1, 2})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, 2.2, got[2].BaseScore)
}

func TestSchedulerScoreReadModelVersioning(t *testing.T) {
	ctx, rdb, model, bucket := newSchedulerScoreReadModelTest(t)
	v1, err := model.NextVersion(ctx, bucket)
	require.NoError(t, err)
	v2, err := model.NextVersion(ctx, bucket)
	require.NoError(t, err)
	require.Greater(t, v2, v1)

	updatedAt := time.Now().UTC()
	require.NoError(t, model.SetBucketScores(ctx, bucket, []service.SchedulerScoreSnapshot{
		{AccountID: 1, Bucket: bucket, GroupID: bucket.GroupID, BaseScore: 1.1, Version: v1, UpdatedAt: updatedAt, BucketSize: 1},
	}, v1, updatedAt))
	require.NoError(t, model.SetBucketScores(ctx, bucket, []service.SchedulerScoreSnapshot{
		{AccountID: 1, Bucket: bucket, GroupID: bucket.GroupID, BaseScore: 2.2, Version: v2, UpdatedAt: updatedAt, BucketSize: 1},
	}, v2, updatedAt))

	active, err := rdb.Get(ctx, schedulerScoreActiveKey(bucket)).Result()
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(v2, 10), active)
	newScoresTTL, err := rdb.TTL(ctx, schedulerScoreScoresKey(bucket, strconv.FormatInt(v2, 10))).Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), newScoresTTL)
	oldScoresTTL, err := rdb.TTL(ctx, schedulerScoreScoresKey(bucket, strconv.FormatInt(v1, 10))).Result()
	require.NoError(t, err)
	require.Greater(t, oldScoresTTL, time.Duration(0))

	require.NoError(t, model.SetBucketScores(ctx, bucket, []service.SchedulerScoreSnapshot{
		{AccountID: 1, Bucket: bucket, GroupID: bucket.GroupID, BaseScore: 0.1, Version: v1, UpdatedAt: updatedAt, BucketSize: 1},
	}, v1, updatedAt))
	active, err = rdb.Get(ctx, schedulerScoreActiveKey(bucket)).Result()
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(v2, 10), active)
	got, err := model.GetScores(ctx, bucket, []int64{1})
	require.NoError(t, err)
	require.Equal(t, 2.2, got[1].BaseScore)
}

func TestSchedulerScoreReadModelNoop(t *testing.T) {
	ctx := context.Background()
	model := NewSchedulerScoreReadModel(nil)
	bucket := service.SchedulerBucket{GroupID: 0, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}

	version, err := model.NextVersion(ctx, bucket)
	require.NoError(t, err)
	require.Zero(t, version)
	require.NoError(t, model.SetBucketScores(ctx, bucket, nil, version, time.Now()))
	require.NoError(t, model.DeleteBucket(ctx, bucket))
	got, err := model.GetScores(ctx, bucket, []int64{1})
	require.NoError(t, err)
	require.Empty(t, got)
}
