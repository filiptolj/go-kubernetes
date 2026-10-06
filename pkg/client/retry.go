package client

import (
	"context"
	"log"
	"time"
)

// RetryDelay is how long Retry waits after a failure before trying again.
var RetryDelay = 2 * time.Second

// Retry calls fn until it succeeds or ctx is cancelled, waiting RetryDelay
// after each failure. Components use it to keep going while the API server
// restarts. It returns nil once fn succeeds, or ctx's error if cancelled.
func Retry(ctx context.Context, what string, fn func() error) error {
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("%s: %v (retrying in %s)", what, err, RetryDelay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(RetryDelay):
		}
	}
}
