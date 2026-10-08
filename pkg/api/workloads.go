package api

import "time"

// Meta is the name and namespace of an object. The newer kinds of object
// embed it, which puts its fields directly in the object (job.Name, not
// job.Meta.Name), also in JSON.
type Meta struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// Job runs pods until a number of them have succeeded: for work that has an
// end, such as a database migration or a report.
type Job struct {
	Meta
	JobSpec

	// Owner is the CronJob that created this Job, if any.
	Owner string `json:"owner,omitempty"`

	Status JobStatus `json:"status"`
}

// JobSpec says what a Job runs, and how often it may try.
type JobSpec struct {
	Completions int `json:"completions,omitempty"` // pods that must succeed; default 1
	Parallelism int `json:"parallelism,omitempty"` // pods running at once; default 1

	// BackoffLimit is how many pods may fail before the whole Job counts as
	// failed; default 6. It is a pointer so that "not set" (nil) can be told
	// apart from 0, which means "no retries at all".
	BackoffLimit *int `json:"backoffLimit,omitempty"`

	// Template is the pod to run. Its restart policy must be OnFailure or
	// Never; the default is Never.
	Template PodTemplate `json:"template"`
}

// Conditions a Job finishes with.
const (
	JobComplete = "Complete"
	JobFailed   = "Failed"
)

// JobStatus is how a Job is doing. The Job controller keeps it up to date.
type JobStatus struct {
	Active    int `json:"active"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`

	// Condition is "" while the Job runs, then JobComplete or JobFailed.
	Condition string `json:"condition,omitempty"`

	StartTime      time.Time `json:"startTime,omitzero"`
	CompletionTime time.Time `json:"completionTime,omitzero"`
}

// CronJob creates a Job on a schedule, like cron on Linux.
type CronJob struct {
	Meta

	// Schedule says when, in cron format: "minute hour day-of-month month
	// day-of-week", such as "*/5 * * * *" for every five minutes.
	Schedule string `json:"schedule"`

	// Suspend pauses the CronJob: no new Jobs are created while it is true.
	Suspend bool `json:"suspend,omitempty"`

	JobTemplate JobSpec       `json:"jobTemplate"`
	Status      CronJobStatus `json:"status"`
}

// CronJobStatus is how a CronJob is doing.
type CronJobStatus struct {
	// LastScheduleTime is the time the latest Job was due.
	LastScheduleTime time.Time `json:"lastScheduleTime,omitzero"`
}

// DaemonSet runs one copy of a pod on every ready node, such as a log
// collector or a monitoring agent.
type DaemonSet struct {
	Meta
	Template PodTemplate `json:"template"`
}

// StatefulSet runs numbered copies of a pod with stable names: web-0, web-1,
// web-2. They are created one at a time, in order, each only once the one
// before is ready, and removed in reverse order. A replaced pod gets the same
// name back. Databases and other apps that care which copy is which use it.
type StatefulSet struct {
	Meta
	Replicas int         `json:"replicas"`
	Template PodTemplate `json:"template"`
}

// ConfigMap holds settings for pods: as environment variables, or as files
// in a volume.
type ConfigMap struct {
	Meta
	Data map[string]string `json:"data"`
}

// Secret is like a ConfigMap, for passwords and keys. minikubectl never
// prints its values, and its files and environment are only readable by
// their owner. Note that, as stored, it is not encrypted.
type Secret struct {
	Meta
	Data map[string]string `json:"data"`
}
