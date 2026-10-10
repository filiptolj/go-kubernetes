package api

import "time"

// Lease records who holds a lock, such as which scheduler is the active
// one. The holder keeps renewing it; if it stops for longer than the lease
// lasts, someone else may take it over. See package leader.
type Lease struct {
	TypeMeta
	ObjectMeta `json:"metadata"`
	LeaseSpec  `json:"spec"`
}

type LeaseSpec struct {
	HolderIdentity       string    `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds int       `json:"leaseDurationSeconds"`
	AcquireTime          time.Time `json:"acquireTime,omitzero"`
	RenewTime            time.Time `json:"renewTime,omitzero"`
	LeaseTransitions     int       `json:"leaseTransitions"` // how often the holder changed
}
