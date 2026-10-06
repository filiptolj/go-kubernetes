package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryUntilSuccess(t *testing.T) {
	RetryDelay = time.Millisecond

	calls := 0
	err := Retry(context.Background(), "test", func() error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	})

	if err != nil {
		t.Errorf("got error %v, want nil", err)
	}
	if calls != 3 {
		t.Errorf("fn was called %d times, want 3", calls)
	}
}

func TestRetryStopsWhenCancelled(t *testing.T) {
	RetryDelay = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := Retry(ctx, "test", func() error {
		calls++
		if calls == 2 {
			cancel()
		}
		return errors.New("always fails")
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("got error %v, want context.Canceled", err)
	}
	if calls != 2 {
		t.Errorf("fn was called %d times, want 2", calls)
	}
}
