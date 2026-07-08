package repository

import (
	"context"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newActiveLoadMapCacheForTest(t *testing.T) (*concurrencyCache, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = rdb.Close()
		mr.Close()
	})
	cache, ok := NewConcurrencyCache(rdb, 15, 900).(*concurrencyCache)
	require.True(t, ok)
	return cache, rdb
}

func TestConcurrencyCacheGetActiveAccountLoadMap_EmptyIndex(t *testing.T) {
	cache, _ := newActiveLoadMapCacheForTest(t)

	loadMap, err := cache.GetActiveAccountLoadMap(context.Background())
	require.NoError(t, err)
	require.Empty(t, loadMap)
}

func TestConcurrencyCacheGetActiveAccountLoadMap_ReturnsSlotAndWaitingLoad(t *testing.T) {
	ctx := context.Background()
	cache, rdb := newActiveLoadMapCacheForTest(t)
	now, err := cache.redisUnixSeconds(ctx)
	require.NoError(t, err)

	require.NoError(t, rdb.ZAdd(ctx, accountSlotKey(101),
		redis.Z{Score: float64(now), Member: "req-1"},
		redis.Z{Score: float64(now), Member: "req-2"},
	).Err())
	require.NoError(t, rdb.Set(ctx, accountWaitKey(101), "3", 0).Err())
	require.NoError(t, rdb.ZAdd(ctx, accountActiveIndexKey, redis.Z{
		Score:  float64(now + 60),
		Member: "101",
	}).Err())

	loadMap, err := cache.GetActiveAccountLoadMap(ctx)
	require.NoError(t, err)
	require.Len(t, loadMap, 1)
	require.Equal(t, int64(101), loadMap[101].AccountID)
	require.Equal(t, 2, loadMap[101].CurrentConcurrency)
	require.Equal(t, 3, loadMap[101].WaitingCount)
	require.Equal(t, 0, loadMap[101].LoadRate)
}

func TestConcurrencyCacheGetActiveAccountLoadMap_RemovesInvalidAndStaleMembers(t *testing.T) {
	ctx := context.Background()
	cache, rdb := newActiveLoadMapCacheForTest(t)
	now, err := cache.redisUnixSeconds(ctx)
	require.NoError(t, err)

	require.NoError(t, rdb.ZAdd(ctx, accountActiveIndexKey,
		redis.Z{Score: float64(now + 60), Member: "bad-id"},
		redis.Z{Score: float64(now + 60), Member: "202"},
	).Err())

	loadMap, err := cache.GetActiveAccountLoadMap(ctx)
	require.NoError(t, err)
	require.Empty(t, loadMap)

	members, err := rdb.ZRange(ctx, accountActiveIndexKey, 0, -1).Result()
	require.NoError(t, err)
	require.Empty(t, members)
}

func TestConcurrencyCacheGetActiveAccountLoadMap_ReadsBeyondChunkWithSameScore(t *testing.T) {
	ctx := context.Background()
	cache, rdb := newActiveLoadMapCacheForTest(t)
	now, err := cache.redisUnixSeconds(ctx)
	require.NoError(t, err)

	count := activeAccountLoadReadChunkSize + 5
	indexMembers := make([]redis.Z, 0, count)
	for i := 0; i < count; i++ {
		accountID := int64(1000 + i)
		require.NoError(t, rdb.Set(ctx, accountWaitKey(accountID), "1", 0).Err())
		indexMembers = append(indexMembers, redis.Z{
			Score:  float64(now + 60),
			Member: strconv.FormatInt(accountID, 10),
		})
	}
	require.NoError(t, rdb.ZAdd(ctx, accountActiveIndexKey, indexMembers...).Err())

	loadMap, err := cache.GetActiveAccountLoadMap(ctx)
	require.NoError(t, err)
	require.Len(t, loadMap, count)
	require.Equal(t, 1, loadMap[int64(1000+count-1)].WaitingCount)
}
