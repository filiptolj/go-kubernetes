package leader

import (
	"context"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/apiserver"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

func elector(c *client.Client, id string) *Elector {
	return &Elector{
		Client: c, Namespace: api.DefaultNamespace, Name: "scheduler", Identity: id,
		LeaseDuration: time.Second, RenewDeadline: 600 * time.Millisecond, RenewEvery: 50 * time.Millisecond,
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestOnlyOneLeadsAndAnotherTakesOver(t *testing.T) {
	ts := httptest.NewServer(apiserver.NewHandler(store.New()))
	defer ts.Close()
	c := client.New(ts.URL)

	var leading atomic.Int32 // how many lead right now
	var mu sync.Mutex
	var leaders []string
	lead := func(id string) func(context.Context) {
		return func(ctx context.Context) {
			if n := leading.Add(1); n > 1 {
				t.Errorf("%d leaders at once", n)
			}
			mu.Lock()
			leaders = append(leaders, id)
			mu.Unlock()
			<-ctx.Done()
			leading.Add(-1)
		}
	}

	ctxA, stopA := context.WithCancel(context.Background())
	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	doneA := make(chan struct{})
	go func() { elector(c, "a").Run(ctxA, lead("a")); close(doneA) }()
	waitFor(t, "a to lead", func() bool { return leading.Load() == 1 })
	go elector(c, "b").Run(ctxB, lead("b"))

	// b waits while a leads.
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	if len(leaders) != 1 || leaders[0] != "a" {
		t.Fatalf("leaders so far: %v, want only a", leaders)
	}
	mu.Unlock()

	// a stops and gives the lease up: b takes over, well before the lease
	// would have run out.
	stopA()
	<-doneA
	start := time.Now()
	waitFor(t, "b to lead", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(leaders) == 2 && leaders[1] == "b"
	})
	if waited := time.Since(start); waited > 500*time.Millisecond {
		t.Errorf("b took over after %s; a gave the lease up, so it shouldn't wait", waited)
	}
	lease, _ := c.Leases().Get(api.DefaultNamespace, "scheduler")
	if lease.HolderIdentity != "b" || lease.LeaseTransitions != 1 {
		t.Errorf("got lease %+v, want held by b after 1 transition", lease.LeaseSpec)
	}
}

func TestExpiredLeaseIsTakenOver(t *testing.T) {
	ts := httptest.NewServer(apiserver.NewHandler(store.New()))
	defer ts.Close()
	c := client.New(ts.URL)

	// A leader that crashed: its lease is there, but nobody renews it.
	c.Leases().Create(api.DefaultNamespace, api.Lease{
		ObjectMeta: api.ObjectMeta{Name: "scheduler"},
		LeaseSpec:  api.LeaseSpec{HolderIdentity: "crashed", LeaseDurationSeconds: 1, RenewTime: time.Now().UTC()},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan time.Time, 1)
	start := time.Now()
	go elector(c, "b").Run(ctx, func(ctx context.Context) { started <- time.Now(); <-ctx.Done() })

	select {
	case at := <-started:
		if at.Sub(start) < 500*time.Millisecond {
			t.Errorf("took over after %s, before the crashed leader's lease ran out", at.Sub(start))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("never took over the expired lease")
	}
}
