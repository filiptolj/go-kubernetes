// Package leader lets several copies of a component run while only one of
// them works: the leader. The others wait, ready to take over if it stops.
//
// It works like Kubernetes' leader election, with a Lease object: the
// leader writes its name and the time into the Lease every RenewEvery. A
// copy that finds the Lease not renewed for LeaseDuration takes it over.
// Two copies trying at once can't both win: each writes the Lease with the
// resourceVersion it read, and the API server refuses the second write as a
// conflict.
package leader

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// Elector takes part in the election for one Lease.
type Elector struct {
	Client    *client.Client
	Namespace string // where the Lease lives
	Name      string // the Lease's name, such as "scheduler"
	Identity  string // who we are; see DefaultIdentity

	// LeaseDuration is how long a Lease stays valid after it was last
	// renewed; RenewEvery is how often the leader renews it. A leader that
	// can't renew it for RenewDeadline stops leading, before LeaseDuration
	// runs out and someone else might start.
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RenewEvery    time.Duration
}

// DefaultIdentity is a name for this process: the host's name and the
// process's ID.
func DefaultIdentity() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s_%d", host, os.Getpid())
}

// Run takes part in the election until ctx is cancelled. Whenever it wins,
// it calls lead with a context that is cancelled when the leadership ends,
// and waits for lead to return. Then it goes back to waiting for its turn.
// When ctx is cancelled while leading, it gives the Lease up, so another
// copy can take over at once instead of waiting for it to run out.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context)) {
	for ctx.Err() == nil {
		if !e.acquire(ctx) {
			return
		}
		log.Printf("leader election: %s is now the leader for %q", e.Identity, e.Name)

		leadCtx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			lead(leadCtx)
		}()

		e.renew(leadCtx)
		stop()
		<-done

		if ctx.Err() != nil {
			e.release()
			return
		}
		log.Printf("leader election: %s lost the leadership for %q", e.Identity, e.Name)
	}
}

// acquire tries to take the Lease every RenewEvery until it does, or ctx is
// cancelled. It reports whether it got the Lease.
func (e *Elector) acquire(ctx context.Context) bool {
	waitingFor := ""
	for {
		holder, ok := e.tryAcquireOrRenew()
		if ok {
			return true
		}
		if holder != waitingFor && holder != "" {
			log.Printf("leader election: %q is led by %s; waiting", e.Name, holder)
			waitingFor = holder
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(e.RenewEvery):
		}
	}
}

// renew renews the Lease every RenewEvery, until ctx is cancelled or a
// renewal hasn't succeeded for RenewDeadline.
func (e *Elector) renew(ctx context.Context) {
	lastRenewed := time.Now()
	ticker := time.NewTicker(e.RenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if _, ok := e.tryAcquireOrRenew(); ok {
			lastRenewed = time.Now()
		} else if time.Since(lastRenewed) > e.RenewDeadline {
			return
		}
	}
}

// tryAcquireOrRenew writes us into the Lease as its holder, if it is free,
// expired, or already ours. Otherwise it returns who holds it.
func (e *Elector) tryAcquireOrRenew() (holder string, ok bool) {
	now := time.Now().UTC()
	lease, err := e.Client.Leases().Get(e.Namespace, e.Name)
	if errors.Is(err, client.ErrNotFound) {
		lease = api.Lease{
			TypeMeta:   api.TypeMetaFor("Lease"),
			ObjectMeta: api.ObjectMeta{Name: e.Name, Namespace: e.Namespace},
			LeaseSpec: api.LeaseSpec{HolderIdentity: e.Identity, LeaseDurationSeconds: e.seconds(),
				AcquireTime: now, RenewTime: now},
		}
		err := e.Client.Leases().Create(e.Namespace, lease)
		return e.Identity, err == nil // a conflict means someone else created it first
	}
	if err != nil {
		return "", false
	}

	expires := lease.RenewTime.Add(time.Duration(lease.LeaseDurationSeconds) * time.Second)
	held := lease.HolderIdentity != "" && now.Before(expires)
	if held && lease.HolderIdentity != e.Identity {
		return lease.HolderIdentity, false
	}

	if lease.HolderIdentity != e.Identity {
		lease.HolderIdentity = e.Identity
		lease.AcquireTime = now
		lease.LeaseTransitions++
	}
	lease.RenewTime = now
	lease.LeaseDurationSeconds = e.seconds()

	// lease carries the resourceVersion we read: if anyone changed it since,
	// this is refused, and they won.
	err = e.Client.Leases().Update(e.Namespace, e.Name, lease)
	return e.Identity, err == nil
}

// seconds returns LeaseDuration in whole seconds, as a Lease keeps it,
// rounded up: rounded down, a short lease would expire at once.
func (e *Elector) seconds() int {
	return max(1, int(math.Ceil(e.LeaseDuration.Seconds())))
}

// release gives the Lease up, if we hold it, so another copy can take over
// without waiting for it to expire.
func (e *Elector) release() {
	lease, err := e.Client.Leases().Get(e.Namespace, e.Name)
	if err != nil || lease.HolderIdentity != e.Identity {
		return
	}
	lease.HolderIdentity = ""
	lease.RenewTime = time.Time{}
	if e.Client.Leases().Update(e.Namespace, e.Name, lease) == nil {
		log.Printf("leader election: %s gave up the leadership for %q", e.Identity, e.Name)
	}
}
