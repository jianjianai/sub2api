package handler

import (
	"errors"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func classifySchedulerCacheError(err error) (status int, errType string, message string, retryAfter string, ok bool) {
	switch {
	case errors.Is(err, service.ErrSchedulerCacheNotReady):
		return http.StatusServiceUnavailable, "scheduler_cache_not_ready", "Scheduler cache not ready", "1", true
	case errors.Is(err, service.ErrSchedulerCacheUnavailable):
		return http.StatusServiceUnavailable, "scheduler_cache_unavailable", "Scheduler cache unavailable", "1", true
	default:
		return 0, "", "", "", false
	}
}
