package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	SchedulerModeSingle = "single"
	SchedulerModeMixed  = "mixed"
	SchedulerModeForced = "forced"
)

const (
	DefaultSchedulerCandidateFetchLimit     = 64
	MinSchedulerCandidateFetchLimit         = 8
	DefaultSchedulerCandidateRestoreBatch   = 100
	DefaultSchedulerCandidateRestoreDelayMS = 1000
	DefaultSchedulerCandidateReadyWaitMS    = 200
	DefaultSchedulerCandidateBuildWaitMS    = 5000
)

const (
	SchedulerCandidateStatusDisabled = "disabled"
	SchedulerCandidateStatusBuilding = "building"
	SchedulerCandidateStatusActive   = "active"
	SchedulerCandidateStatusFailed   = "failed"
)

type SchedulerBucket struct {
	GroupID  int64
	Platform string
	Mode     string
}

func (b SchedulerBucket) String() string {
	return fmt.Sprintf("%d:%s:%s", b.GroupID, b.Platform, b.Mode)
}

func ParseSchedulerBucket(raw string) (SchedulerBucket, bool) {
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return SchedulerBucket{}, false
	}
	groupID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return SchedulerBucket{}, false
	}
	if parts[1] == "" || parts[2] == "" {
		return SchedulerBucket{}, false
	}
	return SchedulerBucket{
		GroupID:  groupID,
		Platform: parts[1],
		Mode:     parts[2],
	}, true
}

type SchedulerCandidateListOptions struct {
	Limit int
}

type SchedulerBlockedAccountState struct {
	AccountID int64
	Until     time.Time
	Reason    string
	Source    string
	UpdatedAt time.Time
}

type SchedulerCandidateIndexSwitchState struct {
	Enabled   bool
	Status    string
	LastError string
}

// SchedulerCache 负责调度快照与账号快照的缓存读写。
type SchedulerCache interface {
	// GetSnapshot 读取快照并返回命中与否（ready + active + 数据完整）。
	GetSnapshot(ctx context.Context, bucket SchedulerBucket) ([]*Account, bool, error)
	// SetSnapshot 写入快照并切换激活版本。
	SetSnapshot(ctx context.Context, bucket SchedulerBucket, accounts []Account) error
	// SetCandidateIndex 写入候选索引，不写旧 snapshot key。
	SetCandidateIndex(ctx context.Context, bucket SchedulerBucket, accounts []Account) error
	// DeleteOldSnapshots 删除旧 snapshot key，保留账号快照和 candidate index。
	DeleteOldSnapshots(ctx context.Context, buckets []SchedulerBucket) error
	// GetAccount 获取单账号快照。
	GetAccount(ctx context.Context, accountID int64) (*Account, error)
	// SetAccount 写入单账号快照（包含不可调度状态）。
	SetAccount(ctx context.Context, account *Account) error
	// DeleteAccount 删除单账号快照。
	DeleteAccount(ctx context.Context, accountID int64) error
	// UpdateLastUsed 批量更新账号的最后使用时间。
	UpdateLastUsed(ctx context.Context, updates map[int64]time.Time) error
	// ListCandidateAccounts 从候选索引读取固定小批量账号元数据。
	ListCandidateAccounts(ctx context.Context, bucket SchedulerBucket, opts SchedulerCandidateListOptions) ([]*Account, bool, error)
	// RemoveAccountFromCandidates 从所有反向索引 bucket 中移除账号，并记录延迟恢复状态。
	RemoveAccountFromCandidates(ctx context.Context, accountID int64, state SchedulerBlockedAccountState) ([]SchedulerBucket, error)
	// RestoreAccountCandidates 将已确认可调度账号恢复到候选索引。
	RestoreAccountCandidates(ctx context.Context, account *Account, buckets []SchedulerBucket) error
	// UpdateCandidateScores 批量更新候选索引分数和账号 LastUsedAt 快照。
	UpdateCandidateScores(ctx context.Context, updates map[int64]time.Time) error
	// PopDueBlockedAccounts 读取到期的 blocked 队列成员，不删除。
	PopDueBlockedAccounts(ctx context.Context, now time.Time, limit int) ([]int64, error)
	// AckBlockedAccount 确认 blocked 账号已处理。
	AckBlockedAccount(ctx context.Context, accountID int64) error
	// RequeueBlockedAccount 将 blocked 账号重新入队。
	RequeueBlockedAccount(ctx context.Context, accountID int64, until time.Time, reason string) error
	// TryLockBucket 尝试获取分桶重建锁。
	TryLockBucket(ctx context.Context, bucket SchedulerBucket, ttl time.Duration) (bool, error)
	// UnlockBucket 释放分桶重建锁。
	UnlockBucket(ctx context.Context, bucket SchedulerBucket) error
	// ListBuckets 返回已注册的分桶集合。
	ListBuckets(ctx context.Context) ([]SchedulerBucket, error)
	// GetOutboxWatermark 读取 outbox 水位。
	GetOutboxWatermark(ctx context.Context) (int64, error)
	// SetOutboxWatermark 保存 outbox 水位。
	SetOutboxWatermark(ctx context.Context, id int64) error
}
