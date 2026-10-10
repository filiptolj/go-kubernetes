package store

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// The API server records a few things about every object in its metadata:
// a UID, when it was created, and a resourceVersion that changes with every
// change. The helpers below fill them in. They all expect the caller to hold
// s.mu.

// nextVersion returns a new resourceVersion. Versions come from one counter
// for the whole store, so each change gets a higher number than any before
// it.
func (s *Store) nextVersion() string {
	s.version++
	return strconv.FormatUint(s.version, 10)
}

// seeVersion makes sure the counter is past a version loaded from the
// backend, so new versions are always higher than saved ones.
func (s *Store) seeVersion(version string) {
	v, err := strconv.ParseUint(version, 10, 64)
	if err == nil && v > s.version {
		s.version = v
	}
}

// markForDeletion marks an object as being deleted, if it isn't yet: it
// gets a DeletionTimestamp and a new resourceVersion.
func (s *Store) markForDeletion(m *api.ObjectMeta) {
	if m.DeletionTimestamp == nil {
		now := time.Now().UTC().Truncate(time.Second)
		m.DeletionTimestamp = &now
	}
	m.ResourceVersion = s.nextVersion()
}

// stampNew fills in the metadata of a new object.
func (s *Store) stampNew(m *api.ObjectMeta) {
	// rand.Text returns a random string: 26 letters and digits, too many
	// combinations for two objects to ever get the same one.
	m.UID = rand.Text()
	m.CreationTimestamp = time.Now().UTC().Truncate(time.Second)
	m.ResourceVersion = s.nextVersion()
}

// stampUpdate checks an update against the stored object, and fills in the
// metadata of the new version.
//
// If the update carries a resourceVersion, it must be the stored one:
// otherwise the update was made from an older copy and would undo whatever
// changed since, so it is refused with ErrConflict. An update without one
// (as from `minikubectl apply`) always goes through.
func (s *Store) stampUpdate(m *api.ObjectMeta, stored api.ObjectMeta) error {
	if m.ResourceVersion != "" && m.ResourceVersion != stored.ResourceVersion {
		return fmt.Errorf("the object has been modified since it was read (version %s, now %s); read it again and retry: %w",
			m.ResourceVersion, stored.ResourceVersion, ErrConflict)
	}

	// Clients don't get to change what the API server records.
	m.UID = stored.UID
	m.CreationTimestamp = stored.CreationTimestamp
	if m.OwnerReferences == nil {
		m.OwnerReferences = stored.OwnerReferences
	}
	// Only a delete can mark an object for deletion, and nothing can take
	// the mark back.
	m.DeletionTimestamp = stored.DeletionTimestamp
	m.DeletionGracePeriodSeconds = stored.DeletionGracePeriodSeconds
	m.ResourceVersion = s.nextVersion()
	return nil
}
