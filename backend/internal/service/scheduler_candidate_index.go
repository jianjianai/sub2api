package service

import (
	"context"
	"sort"
	"time"
)

type SchedulerCandidateIndexMeta struct {
	Bucket     SchedulerBucket `json:"bucket"`
	BucketSize int             `json:"bucket_size"`
	UpdatedAt  time.Time       `json:"updated_at"`
	Version    int64           `json:"version"`
}

type SchedulerCandidatePage struct {
	Accounts []*Account
	Meta     SchedulerCandidateIndexMeta
	Start    int
	End      int
	HasMore  bool
	Hit      bool
}

type SchedulerCandidateIndexReadModel interface {
	NextVersion(ctx context.Context, bucket SchedulerBucket) (int64, error)
	GetCandidatePage(ctx context.Context, bucket SchedulerBucket, start int, limit int) (SchedulerCandidatePage, error)
	SetBucketCandidates(ctx context.Context, bucket SchedulerBucket, accounts []Account, version int64, updatedAt time.Time) error
	DeleteBucket(ctx context.Context, bucket SchedulerBucket) error
}

func BuildSchedulerCandidateIndexAccounts(bucket SchedulerBucket, accounts []Account) []Account {
	out := make([]Account, 0, len(accounts))
	for _, account := range accounts {
		if account.ID <= 0 {
			continue
		}
		out = append(out, buildSchedulerCandidateIndexAccount(account))
	}
	sortSchedulerCandidateIndexAccounts(bucket, out)
	return out
}

func buildSchedulerCandidateIndexAccount(account Account) Account {
	return Account{
		ID:                      account.ID,
		Name:                    account.Name,
		Platform:                account.Platform,
		Type:                    account.Type,
		Concurrency:             account.Concurrency,
		LoadFactor:              account.LoadFactor,
		Priority:                account.Priority,
		RateMultiplier:          account.RateMultiplier,
		Status:                  account.Status,
		LastUsedAt:              account.LastUsedAt,
		ExpiresAt:               account.ExpiresAt,
		AutoPauseOnExpired:      account.AutoPauseOnExpired,
		Schedulable:             account.Schedulable,
		RateLimitedAt:           account.RateLimitedAt,
		RateLimitResetAt:        account.RateLimitResetAt,
		OverloadUntil:           account.OverloadUntil,
		TempUnschedulableUntil:  account.TempUnschedulableUntil,
		TempUnschedulableReason: account.TempUnschedulableReason,
		SessionWindowStart:      account.SessionWindowStart,
		SessionWindowEnd:        account.SessionWindowEnd,
		SessionWindowStatus:     account.SessionWindowStatus,
		ParentAccountID:         account.ParentAccountID,
		QuotaDimension:          account.QuotaDimension,
		AccountGroups:           cloneSchedulerCandidateAccountGroups(account.AccountGroups),
		GroupIDs:                cloneSchedulerCandidateGroupIDs(account.GroupIDs, account.AccountGroups),
		Credentials:             filterSchedulerCandidateCredentials(account.Credentials),
		Extra:                   filterSchedulerCandidateExtra(account.Extra),
	}
}

func sortSchedulerCandidateIndexAccounts(bucket SchedulerBucket, accounts []Account) {
	sort.SliceStable(accounts, func(i, j int) bool {
		leftPriority := schedulerCandidateGroupPriority(bucket, accounts[i])
		rightPriority := schedulerCandidateGroupPriority(bucket, accounts[j])
		if compareOptionalPriority(leftPriority, rightPriority) != 0 {
			return compareOptionalPriority(leftPriority, rightPriority) < 0
		}
		if accounts[i].Priority != accounts[j].Priority {
			return accounts[i].Priority < accounts[j].Priority
		}
		switch {
		case accounts[i].LastUsedAt == nil && accounts[j].LastUsedAt != nil:
			return true
		case accounts[i].LastUsedAt != nil && accounts[j].LastUsedAt == nil:
			return false
		case accounts[i].LastUsedAt != nil && accounts[j].LastUsedAt != nil && !accounts[i].LastUsedAt.Equal(*accounts[j].LastUsedAt):
			return accounts[i].LastUsedAt.Before(*accounts[j].LastUsedAt)
		}
		return accounts[i].ID < accounts[j].ID
	})
}

func schedulerCandidateGroupPriority(bucket SchedulerBucket, account Account) *int {
	if bucket.GroupID <= 0 {
		return nil
	}
	for _, accountGroup := range account.AccountGroups {
		if accountGroup.GroupID == bucket.GroupID {
			priority := accountGroup.Priority
			return &priority
		}
	}
	return nil
}

func cloneSchedulerCandidateAccountGroups(accountGroups []AccountGroup) []AccountGroup {
	if len(accountGroups) == 0 {
		return nil
	}
	out := make([]AccountGroup, 0, len(accountGroups))
	for _, accountGroup := range accountGroups {
		if accountGroup.GroupID <= 0 {
			continue
		}
		out = append(out, AccountGroup{
			AccountID: accountGroup.AccountID,
			GroupID:   accountGroup.GroupID,
			Priority:  accountGroup.Priority,
			CreatedAt: accountGroup.CreatedAt,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneSchedulerCandidateGroupIDs(groupIDs []int64, accountGroups []AccountGroup) []int64 {
	if len(groupIDs) == 0 && len(accountGroups) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(groupIDs)+len(accountGroups))
	out := make([]int64, 0, len(groupIDs)+len(accountGroups))
	for _, id := range groupIDs {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, accountGroup := range accountGroups {
		if accountGroup.GroupID <= 0 {
			continue
		}
		if _, ok := seen[accountGroup.GroupID]; ok {
			continue
		}
		seen[accountGroup.GroupID] = struct{}{}
		out = append(out, accountGroup.GroupID)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func filterSchedulerCandidateCredentials(credentials map[string]any) map[string]any {
	if len(credentials) == 0 {
		return nil
	}
	// 候选索引只保存请求时过滤需要的调度元数据，严禁写入 api_key/access_token/refresh_token 等真实凭据。
	keys := []string{
		"model_mapping",
		"compact_model_mapping",
		openAIEndpointCapabilitiesCredentialKey,
		openAIAuthModeCredentialKey,
		openAIAuthModeLegacyCredentialKey,
		"base_url",
		"project_id",
		"oauth_type",
		"plan_type",
	}
	return filterSchedulerCandidateMap(credentials, keys)
}

func filterSchedulerCandidateExtra(extra map[string]any) map[string]any {
	if len(extra) == 0 {
		return nil
	}
	keys := []string{
		"mixed_scheduling",
		"window_cost_limit",
		"window_cost_sticky_reserve",
		"max_sessions",
		"session_idle_timeout_minutes",
		"openai_compact_mode",
		"openai_compact_supported",
		"openai_oauth_responses_websockets_v2_enabled",
		"openai_oauth_responses_websockets_v2_mode",
		"openai_apikey_responses_websockets_v2_enabled",
		"openai_apikey_responses_websockets_v2_mode",
		"responses_websockets_v2_enabled",
		"openai_ws_enabled",
		"openai_ws_force_http",
		"openai_responses_mode",
		"openai_responses_supported",
		"codex_5h_used_percent",
		"codex_7d_used_percent",
		"codex_5h_reset_at",
		"codex_7d_reset_at",
		"codex_5h_reset_after_seconds",
		"codex_7d_reset_after_seconds",
		"codex_usage_updated_at",
		"auto_pause_5h_threshold",
		"auto_pause_7d_threshold",
		"auto_pause_5h_disabled",
		"auto_pause_7d_disabled",
		"model_rate_limits",
	}
	return filterSchedulerCandidateMap(extra, keys)
}

func filterSchedulerCandidateMap(in map[string]any, keys []string) map[string]any {
	out := make(map[string]any)
	for _, key := range keys {
		if value, ok := in[key]; ok && value != nil {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
