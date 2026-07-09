package service

import (
	"context"
	"log/slog"
	"time"
)

type accountCandidateIndexBlocker interface {
	BlockAccountCandidates(ctx context.Context, account *Account, until time.Time, reason string, source string)
	RestoreAccountCandidates(ctx context.Context, accountID int64, source string) error
}

// notifyCandidateIndexBlocked 是对 AccountRuntimeBlocker 的可选扩展：
// 老测试 stub 只实现 L1 runtime block 也能工作，真实 OpenAIGatewayService 会额外维护 Redis 候选索引。
func notifyCandidateIndexBlocked(ctx context.Context, blocker AccountRuntimeBlocker, account *Account, until time.Time, reason string, source string) {
	if blocker == nil || account == nil {
		return
	}
	candidateBlocker, ok := blocker.(accountCandidateIndexBlocker)
	if !ok {
		return
	}
	candidateBlocker.BlockAccountCandidates(ctx, account, until, reason, source)
}

// notifyCandidateIndexRestored 只发起恢复尝试，是否真的回池由 RestoreAccountCandidates
// 重读 DB 后决定，避免清理某个状态时误恢复仍不可调度的账号。
func notifyCandidateIndexRestored(ctx context.Context, blocker AccountRuntimeBlocker, accountID int64, source string) {
	if blocker == nil || accountID <= 0 {
		return
	}
	candidateBlocker, ok := blocker.(accountCandidateIndexBlocker)
	if !ok {
		return
	}
	if err := candidateBlocker.RestoreAccountCandidates(ctx, accountID, source); err != nil {
		slog.Warn("scheduler_candidate_index_restore_failed",
			"account_id", accountID,
			"source", source,
			"error", err,
		)
	}
}

// BlockAccountCandidates 先写本机 L1 runtime block，再从 Redis candidate index 删除。
// L1 优先保证本进程立即避开该账号；Redis 删除保证其他进程后续选号也避开。
func (s *OpenAIGatewayService) BlockAccountCandidates(ctx context.Context, account *Account, until time.Time, reason string, source string) {
	if s == nil || account == nil || account.ID <= 0 || !isOpenAIAccount(account) {
		return
	}

	blockUntil := until
	now := time.Now()
	if blockUntil.IsZero() || !blockUntil.After(now) {
		blockUntil = now.Add(openAIStopSchedulingBridgeCooldown)
	}
	s.BlockAccountScheduling(account, blockUntil, reason)

	if s.schedulerSnapshot == nil || s.schedulerSnapshot.cache == nil {
		return
	}
	state := SchedulerBlockedAccountState{
		AccountID: account.ID,
		Until:     blockUntil,
		Reason:    reason,
		Source:    source,
		UpdatedAt: now,
	}
	removed, err := s.schedulerSnapshot.cache.RemoveAccountFromCandidates(ctx, account.ID, state)
	if err != nil {
		slog.Warn("scheduler_candidate_index_remove_failed",
			"account_id", account.ID,
			"reason", reason,
			"source", source,
			"error", err,
		)
		return
	}
	slog.Debug("scheduler_candidate_index_remove",
		"account_id", account.ID,
		"bucket_count", len(removed),
		"reason", reason,
		"source", source,
	)
}

// RestoreAccountCandidates 以 DB 最新状态为准：账号仍不可调度则重排 blocked 队列，
// 只有 IsSchedulable 通过后才重新计算 bucket 并加入 candidate index。
func (s *OpenAIGatewayService) RestoreAccountCandidates(ctx context.Context, accountID int64, source string) error {
	if s == nil || accountID <= 0 || s.schedulerSnapshot == nil || s.schedulerSnapshot.cache == nil {
		return nil
	}
	cache := s.schedulerSnapshot.cache
	if s.accountRepo == nil {
		return cache.AckBlockedAccount(ctx, accountID)
	}

	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil || account == nil {
		if ackErr := cache.AckBlockedAccount(ctx, accountID); ackErr != nil {
			return ackErr
		}
		return err
	}
	if !isOpenAIAccount(account) {
		return cache.AckBlockedAccount(ctx, accountID)
	}
	if !account.IsSchedulable() {
		if until, ok := nextAccountCandidateRestoreTime(account); ok {
			return cache.RequeueBlockedAccount(ctx, accountID, until, source)
		}
		return cache.AckBlockedAccount(ctx, accountID)
	}

	buckets, err := s.schedulerSnapshot.CandidateBucketsForAccount(ctx, account)
	if err != nil {
		return err
	}
	if err := cache.RestoreAccountCandidates(ctx, account, buckets); err != nil {
		slog.Warn("scheduler_candidate_index_restore_failed",
			"account_id", accountID,
			"source", source,
			"error", err,
		)
		return err
	}
	slog.Debug("scheduler_candidate_index_restore",
		"account_id", accountID,
		"bucket_count", len(buckets),
		"source", source,
	)
	return nil
}

// nextAccountCandidateRestoreTime 取账号各类冷却窗口中最早的未来时间，
// 用于 restore 尝试过早时重新入队，避免热循环。
func nextAccountCandidateRestoreTime(account *Account) (time.Time, bool) {
	if account == nil {
		return time.Time{}, false
	}
	now := time.Now()
	var next time.Time
	consider := func(candidate *time.Time) {
		if candidate == nil || !candidate.After(now) {
			return
		}
		if next.IsZero() || candidate.Before(next) {
			next = *candidate
		}
	}
	consider(account.RateLimitResetAt)
	consider(account.OverloadUntil)
	consider(account.TempUnschedulableUntil)
	if account.AutoPauseOnExpired {
		consider(account.ExpiresAt)
	}
	if next.IsZero() {
		return time.Time{}, false
	}
	return next, true
}
