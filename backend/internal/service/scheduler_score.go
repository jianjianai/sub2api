package service

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

const schedulerScoreListReadTimeout = 500 * time.Millisecond

var ErrSchedulerScoreActiveLoadMapEmpty = errors.New("scheduler score active load map empty for large bucket")

type SchedulerScoreSnapshot struct {
	AccountID             int64           `json:"account_id"`
	Bucket                SchedulerBucket `json:"bucket"`
	GroupID               int64           `json:"group_id"`
	GroupName             string          `json:"group_name,omitempty"`
	GroupPriority         *int            `json:"group_priority,omitempty"`
	BaseScore             float64         `json:"base_score"`
	StickyScore           float64         `json:"sticky_score"`
	StickyScoreInfinity   bool            `json:"sticky_score_infinity"`
	StickyWeightedEnabled bool            `json:"sticky_weighted_enabled"`
	Rank                  int             `json:"rank"`
	BucketSize            int             `json:"bucket_size"`
	UpdatedAt             time.Time       `json:"updated_at"`
	Version               int64           `json:"version"`
}

type SchedulerScoreReadModel interface {
	NextVersion(ctx context.Context, bucket SchedulerBucket) (int64, error)
	GetScores(ctx context.Context, bucket SchedulerBucket, accountIDs []int64) (map[int64]SchedulerScoreSnapshot, error)
	SetBucketScores(ctx context.Context, bucket SchedulerBucket, scores []SchedulerScoreSnapshot, version int64, updatedAt time.Time) error
	DeleteBucket(ctx context.Context, bucket SchedulerBucket) error
}

type SchedulerScoreService struct {
	readModel          SchedulerScoreReadModel
	concurrencyService *ConcurrencyService
	rateLimitService   *RateLimitService
	cfg                *config.Config
}

func NewSchedulerScoreService(
	readModel SchedulerScoreReadModel,
	concurrencyService *ConcurrencyService,
	rateLimitService *RateLimitService,
	cfg *config.Config,
) *SchedulerScoreService {
	return &SchedulerScoreService{
		readModel:          readModel,
		concurrencyService: concurrencyService,
		rateLimitService:   rateLimitService,
		cfg:                cfg,
	}
}

type AccountListSchedulerScore struct {
	AccountID             int64
	GroupID               *int64
	GroupName             string
	GroupPriority         *int
	BaseScore             float64
	StickyScore           float64
	StickyScoreInfinity   bool
	StickyWeightedEnabled bool
	Rank                  int
	BucketSize            int
	UpdatedAt             time.Time
	Missing               bool
}

func (s *SchedulerScoreService) RebuildBucketScores(ctx context.Context, bucket SchedulerBucket, accounts []Account) error {
	if s == nil || s.readModel == nil {
		return nil
	}
	bucket = s.normalizeBucket(bucket)
	if bucket.Platform != PlatformOpenAI || bucket.Mode != SchedulerModeSingle {
		return s.readModel.DeleteBucket(ctx, bucket)
	}
	if len(accounts) == 0 {
		return s.readModel.DeleteBucket(ctx, bucket)
	}

	activeLoadMap := map[int64]*AccountLoadInfo{}
	if s.concurrencyService != nil {
		loadMap, err := s.concurrencyService.GetActiveAccountLoadMap(ctx)
		if err != nil {
			slog.Warn("scheduler_score_active_load_map_failed", "bucket", bucket.String(), "error", err)
			return err
		}
		if len(loadMap) == 0 && len(accounts) > 100 {
			// 大桶下 active index 为空通常意味着 Redis 负载索引异常；保留旧版本比发布“全员零负载”更安全。
			slog.Warn("active_load_map_empty_for_large_bucket", "bucket", bucket.String(), "bucket_size", len(accounts))
			return ErrSchedulerScoreActiveLoadMapEmpty
		}
		activeLoadMap = loadMap
	}

	openAIAccounts := make([]*Account, 0, len(accounts))
	bucketLoadMap := make(map[int64]*AccountLoadInfo, len(accounts))
	for i := range accounts {
		account := &accounts[i]
		if account.Platform != PlatformOpenAI {
			continue
		}
		openAIAccounts = append(openAIAccounts, account)
		bucketLoadMap[account.ID] = buildSchedulerScoreLoadInfo(account, activeLoadMap[account.ID])
	}
	if len(openAIAccounts) == 0 {
		return s.readModel.DeleteBucket(ctx, bucket)
	}

	scoreByAccount := map[int64]OpenAIAccountSchedulerScoreSnapshot{}
	if s.rateLimitService != nil {
		scoreByAccount = s.rateLimitService.BuildOpenAIAccountSchedulerScoreSnapshot(ctx, openAIAccounts, bucketLoadMap)
	} else {
		scoreByAccount = BuildOpenAIAccountSchedulerScoreSnapshot(openAIAccounts, bucketLoadMap)
	}
	if len(scoreByAccount) == 0 {
		return s.readModel.DeleteBucket(ctx, bucket)
	}

	version, err := s.readModel.NextVersion(ctx, bucket)
	if err != nil {
		return err
	}
	updatedAt := time.Now().UTC()
	rows := make([]SchedulerScoreSnapshot, 0, len(scoreByAccount))
	accountByID := make(map[int64]*Account, len(openAIAccounts))
	for _, account := range openAIAccounts {
		accountByID[account.ID] = account
	}
	for accountID, score := range scoreByAccount {
		account := accountByID[accountID]
		if account == nil {
			continue
		}
		groupName, groupPriority := schedulerScoreGroupMetadata(account, bucket.GroupID)
		rows = append(rows, SchedulerScoreSnapshot{
			AccountID:             accountID,
			Bucket:                bucket,
			GroupID:               bucket.GroupID,
			GroupName:             groupName,
			GroupPriority:         groupPriority,
			BaseScore:             score.BaseScore,
			StickyScore:           score.StickyScore,
			StickyScoreInfinity:   score.StickyScoreInfinity,
			StickyWeightedEnabled: score.StickyWeightedEnabled,
			BucketSize:            len(openAIAccounts),
			UpdatedAt:             updatedAt,
			Version:               version,
		})
	}
	sortSchedulerScoreSnapshots(rows, accountByID)
	for i := range rows {
		rows[i].Rank = i + 1
	}
	return s.readModel.SetBucketScores(ctx, bucket, rows, version, updatedAt)
}

func (s *SchedulerScoreService) GetAccountListScores(ctx context.Context, accounts []Account) (map[int64]*AccountListSchedulerScore, map[int64][]AccountListSchedulerScore) {
	baseScores := make(map[int64]*AccountListSchedulerScore)
	groupScores := make(map[int64][]AccountListSchedulerScore)
	if s == nil || s.readModel == nil || len(accounts) == 0 {
		return baseScores, groupScores
	}

	type bucketRead struct {
		bucket     SchedulerBucket
		accountIDs []int64
	}
	readsByBucket := make(map[string]bucketRead)
	for i := range accounts {
		account := &accounts[i]
		if account.Platform != PlatformOpenAI {
			continue
		}
		for _, bucket := range s.accountBuckets(account) {
			key := bucket.String()
			read := readsByBucket[key]
			read.bucket = bucket
			read.accountIDs = append(read.accountIDs, account.ID)
			readsByBucket[key] = read
		}
	}
	if len(readsByBucket) == 0 {
		return baseScores, groupScores
	}

	for _, read := range readsByBucket {
		readCtx, cancel := context.WithTimeout(ctx, schedulerScoreListReadTimeout)
		snapshots, err := s.readModel.GetScores(readCtx, read.bucket, read.accountIDs)
		cancel()
		if err != nil {
			// 列表热路径只读当前页投影；读模型异常时降级为空，禁止回源数据库或全量 Redis。
			slog.Warn("scheduler_score_list_read_failed", "bucket", read.bucket.String(), "error", err)
			continue
		}
		for accountID, snapshot := range snapshots {
			listScore := schedulerScoreSnapshotToListScore(snapshot)
			if snapshot.GroupID > 0 {
				groupScores[accountID] = append(groupScores[accountID], listScore)
				continue
			}
			copied := listScore
			baseScores[accountID] = &copied
		}
	}

	for accountID := range groupScores {
		sortAccountListSchedulerScores(groupScores[accountID])
		if best := bestAccountListSchedulerScore(groupScores[accountID]); best != nil {
			copied := *best
			baseScores[accountID] = &copied
		}
	}
	return baseScores, groupScores
}

func (s *SchedulerScoreService) normalizeBucket(bucket SchedulerBucket) SchedulerBucket {
	if bucket.Mode == "" {
		bucket.Mode = SchedulerModeSingle
	}
	if s != nil && s.cfg != nil && s.cfg.RunMode == config.RunModeSimple {
		bucket.GroupID = 0
	}
	return bucket
}

func (s *SchedulerScoreService) accountBuckets(account *Account) []SchedulerBucket {
	if account == nil || account.Platform != PlatformOpenAI {
		return nil
	}
	if s != nil && s.cfg != nil && s.cfg.RunMode == config.RunModeSimple {
		return []SchedulerBucket{{GroupID: 0, Platform: PlatformOpenAI, Mode: SchedulerModeSingle}}
	}
	groupIDs := accountSchedulerGroupIDs(account)
	buckets := make([]SchedulerBucket, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		buckets = append(buckets, SchedulerBucket{GroupID: groupID, Platform: PlatformOpenAI, Mode: SchedulerModeSingle})
	}
	return buckets
}

func accountSchedulerGroupIDs(account *Account) []int64 {
	if account == nil {
		return nil
	}
	seen := make(map[int64]struct{})
	out := make([]int64, 0, len(account.GroupIDs)+len(account.AccountGroups))
	for _, accountGroup := range account.AccountGroups {
		if accountGroup.GroupID <= 0 {
			continue
		}
		if _, ok := seen[accountGroup.GroupID]; ok {
			continue
		}
		seen[accountGroup.GroupID] = struct{}{}
		out = append(out, accountGroup.GroupID)
	}
	for _, groupID := range account.GroupIDs {
		if groupID <= 0 {
			continue
		}
		if _, ok := seen[groupID]; ok {
			continue
		}
		seen[groupID] = struct{}{}
		out = append(out, groupID)
	}
	if len(out) == 0 {
		return []int64{0}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func buildSchedulerScoreLoadInfo(account *Account, active *AccountLoadInfo) *AccountLoadInfo {
	load := &AccountLoadInfo{AccountID: account.ID}
	if active != nil {
		load.CurrentConcurrency = active.CurrentConcurrency
		load.WaitingCount = active.WaitingCount
	}
	maxConcurrency := account.EffectiveLoadFactor()
	if maxConcurrency > 0 {
		load.LoadRate = (load.CurrentConcurrency + load.WaitingCount) * 100 / maxConcurrency
	}
	return load
}

func schedulerScoreGroupMetadata(account *Account, groupID int64) (string, *int) {
	if account == nil || groupID <= 0 {
		return "", nil
	}
	for _, accountGroup := range account.AccountGroups {
		if accountGroup.GroupID != groupID {
			continue
		}
		priority := accountGroup.Priority
		groupName := ""
		if accountGroup.Group != nil {
			groupName = accountGroup.Group.Name
		}
		return groupName, &priority
	}
	return "", nil
}

func sortSchedulerScoreSnapshots(rows []SchedulerScoreSnapshot, accountByID map[int64]*Account) {
	sort.SliceStable(rows, func(i, j int) bool {
		left := rows[i]
		right := rows[j]
		if left.BaseScore != right.BaseScore {
			return left.BaseScore > right.BaseScore
		}
		if left.StickyScoreInfinity != right.StickyScoreInfinity {
			return left.StickyScoreInfinity
		}
		if left.StickyScore != right.StickyScore {
			return left.StickyScore > right.StickyScore
		}
		if compareOptionalPriority(left.GroupPriority, right.GroupPriority) != 0 {
			return compareOptionalPriority(left.GroupPriority, right.GroupPriority) < 0
		}
		leftAccount := accountByID[left.AccountID]
		rightAccount := accountByID[right.AccountID]
		leftPriority, rightPriority := 0, 0
		if leftAccount != nil {
			leftPriority = leftAccount.Priority
		}
		if rightAccount != nil {
			rightPriority = rightAccount.Priority
		}
		if leftPriority != rightPriority {
			return leftPriority < rightPriority
		}
		return left.AccountID < right.AccountID
	})
}

func schedulerScoreSnapshotToListScore(snapshot SchedulerScoreSnapshot) AccountListSchedulerScore {
	var groupID *int64
	if snapshot.GroupID > 0 {
		gid := snapshot.GroupID
		groupID = &gid
	}
	return AccountListSchedulerScore{
		AccountID:             snapshot.AccountID,
		GroupID:               groupID,
		GroupName:             snapshot.GroupName,
		GroupPriority:         snapshot.GroupPriority,
		BaseScore:             snapshot.BaseScore,
		StickyScore:           snapshot.StickyScore,
		StickyScoreInfinity:   snapshot.StickyScoreInfinity,
		StickyWeightedEnabled: snapshot.StickyWeightedEnabled,
		Rank:                  snapshot.Rank,
		BucketSize:            snapshot.BucketSize,
		UpdatedAt:             snapshot.UpdatedAt,
	}
}

func sortAccountListSchedulerScores(scores []AccountListSchedulerScore) {
	sort.SliceStable(scores, func(i, j int) bool {
		left := scores[i]
		right := scores[j]
		if compareOptionalPriority(left.GroupPriority, right.GroupPriority) != 0 {
			return compareOptionalPriority(left.GroupPriority, right.GroupPriority) < 0
		}
		if left.BaseScore != right.BaseScore {
			return left.BaseScore > right.BaseScore
		}
		leftGroupID, rightGroupID := int64(0), int64(0)
		if left.GroupID != nil {
			leftGroupID = *left.GroupID
		}
		if right.GroupID != nil {
			rightGroupID = *right.GroupID
		}
		return leftGroupID < rightGroupID
	})
}

func bestAccountListSchedulerScore(scores []AccountListSchedulerScore) *AccountListSchedulerScore {
	if len(scores) == 0 {
		return nil
	}
	sortAccountListSchedulerScores(scores)
	return &scores[0]
}

func compareOptionalPriority(left, right *int) int {
	switch {
	case left != nil && right == nil:
		return -1
	case left == nil && right != nil:
		return 1
	case left == nil && right == nil:
		return 0
	case *left < *right:
		return -1
	case *left > *right:
		return 1
	default:
		return 0
	}
}
