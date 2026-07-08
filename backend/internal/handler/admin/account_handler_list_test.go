package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func setupAccountListRouter() (*gin.Engine, *stubAdminService) {
	return setupAccountListRouterWithSchedulerScore(nil)
}

func setupAccountListRouterWithSchedulerScore(schedulerScoreService *service.SchedulerScoreService) (*gin.Engine, *stubAdminService) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	adminSvc := newStubAdminService()
	handler := NewAccountHandler(adminSvc, nil, nil, nil, nil, nil, nil, nil, nil, schedulerScoreService, nil, nil, nil, nil)
	router.GET("/api/v1/admin/accounts", handler.List)
	return router, adminSvc
}

type accountListSchedulerScoreReadModel struct {
	scores map[string]map[int64]service.SchedulerScoreSnapshot
	reads  []accountListScoreRead
}

type accountListScoreRead struct {
	bucket     service.SchedulerBucket
	accountIDs []int64
}

func (m *accountListSchedulerScoreReadModel) NextVersion(context.Context, service.SchedulerBucket) (int64, error) {
	return 1, nil
}

func (m *accountListSchedulerScoreReadModel) GetScores(_ context.Context, bucket service.SchedulerBucket, accountIDs []int64) (map[int64]service.SchedulerScoreSnapshot, error) {
	m.reads = append(m.reads, accountListScoreRead{bucket: bucket, accountIDs: append([]int64(nil), accountIDs...)})
	out := make(map[int64]service.SchedulerScoreSnapshot)
	for _, accountID := range accountIDs {
		if score, ok := m.scores[bucket.String()][accountID]; ok {
			out[accountID] = score
		}
	}
	return out, nil
}

func (m *accountListSchedulerScoreReadModel) SetBucketScores(context.Context, service.SchedulerBucket, []service.SchedulerScoreSnapshot, int64, time.Time) error {
	return nil
}

func (m *accountListSchedulerScoreReadModel) DeleteBucket(context.Context, service.SchedulerBucket) error {
	return nil
}

func TestAccountHandlerListIncludesCreatedAt(t *testing.T) {
	router, adminSvc := setupAccountListRouter()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/accounts?page=1&page_size=20&sort_by=created_at&sort_order=desc", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "created_at", adminSvc.lastListAccounts.sortBy)

	var payload struct {
		Data struct {
			Items []struct {
				ID        int64  `json:"id"`
				CreatedAt string `json:"created_at"`
			} `json:"items"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	require.Len(t, payload.Data.Items, 1)

	createdAt := payload.Data.Items[0].CreatedAt
	require.NotEmpty(t, createdAt)
	require.True(t, strings.HasSuffix(createdAt, "Z"), "created_at should be serialized as UTC")
	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	require.NoError(t, err)
	_, offset := parsed.Zone()
	require.Equal(t, 0, offset)
}

func TestAccountHandlerListReturnsSchedulerScoresFromReadModel(t *testing.T) {
	now := time.Now().UTC()
	groupID := int64(41)
	bucket := service.SchedulerBucket{GroupID: groupID, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	readModel := &accountListSchedulerScoreReadModel{scores: map[string]map[int64]service.SchedulerScoreSnapshot{
		bucket.String(): {
			101: {
				AccountID:             101,
				Bucket:                bucket,
				GroupID:               groupID,
				GroupName:             "openai",
				GroupPriority:         ptrInt(100),
				BaseScore:             3.14,
				StickyScore:           6.14,
				StickyWeightedEnabled: true,
				Rank:                  2,
				BucketSize:            2,
				UpdatedAt:             now,
			},
			102: {
				AccountID:     102,
				Bucket:        bucket,
				GroupID:       groupID,
				GroupName:     "openai",
				GroupPriority: ptrInt(1),
				BaseScore:     4.14,
				Rank:          1,
				BucketSize:    2,
				UpdatedAt:     now,
			},
		},
	}}
	router, adminSvc := setupAccountListRouterWithSchedulerScore(service.NewSchedulerScoreService(readModel, nil, nil, nil))
	adminSvc.accounts = []service.Account{
		{
			ID:          101,
			Name:        "account-high-priority",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeAPIKey,
			Status:      service.StatusActive,
			Schedulable: true,
			Concurrency: 10,
			Priority:    1,
			AccountGroups: []service.AccountGroup{
				{AccountID: 101, GroupID: groupID, Priority: 100, Group: &service.Group{ID: groupID, Name: "openai"}},
			},
			GroupIDs:  []int64{groupID},
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			ID:          102,
			Name:        "account-low-priority",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeAPIKey,
			Status:      service.StatusActive,
			Schedulable: true,
			Concurrency: 10,
			Priority:    100000,
			AccountGroups: []service.AccountGroup{
				{AccountID: 102, GroupID: groupID, Priority: 1, Group: &service.Group{ID: groupID, Name: "openai"}},
			},
			GroupIDs:  []int64{groupID},
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/accounts?page=1&page_size=20&platform=openai&include_scheduler_score=1", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var payload struct {
		Data struct {
			Items []struct {
				ID             int64 `json:"id"`
				SchedulerScore struct {
					BaseScore float64 `json:"base_score"`
				} `json:"scheduler_score"`
				SchedulerScores []struct {
					GroupID       *int64  `json:"group_id"`
					GroupName     string  `json:"group_name"`
					GroupPriority *int    `json:"group_priority"`
					BaseScore     float64 `json:"base_score"`
					StickyScore   float64 `json:"sticky_score"`
					Rank          int     `json:"rank"`
					BucketSize    int     `json:"bucket_size"`
				} `json:"scheduler_scores"`
			} `json:"items"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	require.Len(t, payload.Data.Items, 2)

	var high, low *struct {
		ID             int64 `json:"id"`
		SchedulerScore struct {
			BaseScore float64 `json:"base_score"`
		} `json:"scheduler_score"`
		SchedulerScores []struct {
			GroupID       *int64  `json:"group_id"`
			GroupName     string  `json:"group_name"`
			GroupPriority *int    `json:"group_priority"`
			BaseScore     float64 `json:"base_score"`
			StickyScore   float64 `json:"sticky_score"`
			Rank          int     `json:"rank"`
			BucketSize    int     `json:"bucket_size"`
		} `json:"scheduler_scores"`
	}
	for i := range payload.Data.Items {
		item := &payload.Data.Items[i]
		switch item.ID {
		case 101:
			high = item
		case 102:
			low = item
		}
	}
	require.NotNil(t, high)
	require.NotNil(t, low)
	require.Len(t, high.SchedulerScores, 1)
	require.Len(t, low.SchedulerScores, 1)
	require.Equal(t, groupID, *high.SchedulerScores[0].GroupID)
	require.Equal(t, "openai", high.SchedulerScores[0].GroupName)
	require.Equal(t, 100, *high.SchedulerScores[0].GroupPriority)
	require.Equal(t, 1, *low.SchedulerScores[0].GroupPriority)
	require.Equal(t, 3.14, high.SchedulerScores[0].BaseScore)
	require.Equal(t, 6.14, high.SchedulerScores[0].StickyScore)
	require.Equal(t, 2, high.SchedulerScores[0].Rank)
	require.Equal(t, 2, high.SchedulerScores[0].BucketSize)
	require.Equal(t, 4.14, low.SchedulerScore.BaseScore)
}

func TestAccountHandlerListSkipsSchedulerScoresByDefault(t *testing.T) {
	router, adminSvc := setupAccountListRouter()
	now := time.Now().UTC()
	adminSvc.accounts = []service.Account{
		{
			ID:          110,
			Name:        "openai-account",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeAPIKey,
			Status:      service.StatusActive,
			Schedulable: true,
			Concurrency: 10,
			Priority:    1,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/accounts?page=1&page_size=20&platform=openai", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var payload struct {
		Data struct {
			Items []map[string]any `json:"items"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	require.Len(t, payload.Data.Items, 1)
	require.NotContains(t, payload.Data.Items[0], "scheduler_score")
	require.NotContains(t, payload.Data.Items[0], "scheduler_scores")
}

func TestAccountHandlerListSchedulerScoreOnlyReadsCurrentPageAccounts(t *testing.T) {
	now := time.Now().UTC()
	bucket := service.SchedulerBucket{GroupID: 0, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	readModel := &accountListSchedulerScoreReadModel{scores: map[string]map[int64]service.SchedulerScoreSnapshot{
		bucket.String(): {
			301: {AccountID: 301, Bucket: bucket, BaseScore: 1.1, UpdatedAt: now, BucketSize: 3},
			302: {AccountID: 302, Bucket: bucket, BaseScore: 2.2, UpdatedAt: now, BucketSize: 3},
			303: {AccountID: 303, Bucket: bucket, BaseScore: 3.3, UpdatedAt: now, BucketSize: 3},
		},
	}}
	router, adminSvc := setupAccountListRouterWithSchedulerScore(service.NewSchedulerScoreService(readModel, nil, nil, nil))
	adminSvc.accounts = []service.Account{
		{ID: 301, Name: "page-1-a", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, CreatedAt: now, UpdatedAt: now},
		{ID: 302, Name: "page-1-b", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, CreatedAt: now, UpdatedAt: now},
		{ID: 303, Name: "page-2", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, CreatedAt: now, UpdatedAt: now},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/accounts?page=1&page_size=2&platform=openai&include_scheduler_score=1", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, readModel.reads, 1)
	require.Equal(t, bucket, readModel.reads[0].bucket)
	gotIDs := append([]int64(nil), readModel.reads[0].accountIDs...)
	sort.Slice(gotIDs, func(i, j int) bool { return gotIDs[i] < gotIDs[j] })
	require.Equal(t, []int64{301, 302}, gotIDs)
}

func TestAccountHandlerListSchedulerScoreMissingReadModelProjection(t *testing.T) {
	now := time.Now().UTC()
	readModel := &accountListSchedulerScoreReadModel{scores: map[string]map[int64]service.SchedulerScoreSnapshot{}}
	router, adminSvc := setupAccountListRouterWithSchedulerScore(service.NewSchedulerScoreService(readModel, nil, nil, nil))
	adminSvc.accounts = []service.Account{
		{ID: 401, Name: "missing", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, CreatedAt: now, UpdatedAt: now},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/accounts?page=1&page_size=1&platform=openai&include_scheduler_score=1", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var payload struct {
		Data struct {
			Items []map[string]any `json:"items"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	require.Len(t, payload.Data.Items, 1)
	require.NotContains(t, payload.Data.Items[0], "scheduler_score")
	require.NotContains(t, payload.Data.Items[0], "scheduler_scores")
}

func ptrInt(v int) *int {
	return &v
}
