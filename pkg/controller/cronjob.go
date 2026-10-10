package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/cron"
)

// How many finished Jobs of each CronJob to keep, so their pods' logs can
// still be read. Older ones are deleted.
const (
	keepSucceededJobs = 3
	keepFailedJobs    = 1
)

// CronJobController creates a Job whenever a CronJob is due.
type CronJobController struct {
	Client    *client.Client
	Informers *Informers    // where to read from; nil: from Client
	Every     time.Duration // how often to check

	// Now returns the current time. Tests set it to control the clock; nil
	// means time.Now.
	Now func() time.Time

	expected expectations // Jobs and status changes not seen yet, by CronJob
}

// Run checks the CronJobs whenever a CronJob or a Job changes, and every
// cc.Every (schedules depend on the time), until ctx is cancelled.
func (cc *CronJobController) Run(ctx context.Context) {
	client.RunOnChange(ctx, "cronjob controller", cc.Every, cc.reconcileAll, cc.Informers.cronJobs(), cc.Informers.jobs())
}

func (cc *CronJobController) now() time.Time {
	if cc.Now != nil {
		return cc.Now()
	}
	return time.Now()
}

// reconcileAll checks every CronJob.
func (cc *CronJobController) reconcileAll() {
	cc.expected.check() // before reading: see expectations.go
	cronJobs, err := list(cc.Informers.cronJobs(), cc.Client.CronJobs().List)
	if err != nil {
		log.Printf("cronjob controller: %v", err)
		return
	}
	jobs, err := list(cc.Informers.jobs(), cc.Client.Jobs().List)
	if err != nil {
		log.Printf("cronjob controller: %v", err)
		return
	}

	owned := make(map[string][]api.Job) // by the CronJob's "namespace/name"
	for _, job := range jobs {
		if job.ControlledBy("CronJob") {
			owner := api.Key(job.Namespace, job.OwnerName())
			owned[owner] = append(owned[owner], job)
		}
	}

	for _, cj := range cronJobs {
		key := api.Key(cj.Namespace, cj.Name)
		cc.reconcile(cj, owned[key])
	}

}

// reconcile creates a Job if the CronJob is due, and deletes old finished Jobs.
func (cc *CronJobController) reconcile(cj api.CronJob, jobs []api.Job) {
	if !cc.expected.satisfied(api.Key(cj.Namespace, cj.Name)) {
		return // the informers may not show our last changes yet
	}

	now := cc.now()
	schedule, err := cron.Parse(cj.Schedule)
	if err != nil {
		log.Printf("cronjob %s: %v", api.Key(cj.Namespace, cj.Name), err)
		return
	}

	last := cj.Status.LastScheduleTime
	if last.IsZero() {
		// A new CronJob starts counting from now: times that passed before
		// it existed don't count.
		cc.saveLastSchedule(cj, now.Truncate(time.Minute))
		return
	}

	// Find the latest time that came due since the last run. If the
	// controller was down for a while, several may have passed; like
	// Kubernetes, only the most recent one runs.
	var due time.Time
	for t := schedule.Next(last); !t.IsZero() && !t.After(now); t = schedule.Next(t) {
		due = t
	}

	if !due.IsZero() {
		if !cj.Suspend {
			cc.createJob(cj, due)
		}
		// Even when suspended, move on: resuming must not run what was skipped.
		cc.saveLastSchedule(cj, due)
	}

	cc.cleanUp(jobs)
}

// createJob creates the Job for the time a CronJob was due. The name comes
// from that time, so creating it twice (after a restart, say) fails with a
// conflict instead of running the work twice.
func (cc *CronJobController) createJob(cj api.CronJob, due time.Time) {
	name := fmt.Sprintf("%s-%d", cj.Name, due.Unix()/60)
	job := api.Job{
		TypeMeta:   api.TypeMetaFor("Job"),
		ObjectMeta: api.ObjectMeta{Name: name, Namespace: cj.Namespace},
		JobSpec:    cj.JobTemplate.Spec,
	}
	job.SetOwner("CronJob", cj.Name, cj.UID)

	err := cc.Client.Jobs().Create(cj.Namespace, job)
	if errors.Is(err, client.ErrConflict) {
		return // already created
	}
	if err != nil {
		log.Printf("cronjob %s: %v", api.Key(cj.Namespace, cj.Name), err)
		return
	}
	expectPresent(&cc.expected, cc.Informers.jobs(), api.Key(cj.Namespace, cj.Name), cj.Namespace, name)
	log.Printf("cronjob %s: created job %q", api.Key(cj.Namespace, cj.Name), name)
	cc.events().Normal("CronJob", cj.Namespace, cj.Name, "SuccessfulCreate", "created job %q", name)
}

func (cc *CronJobController) saveLastSchedule(cj api.CronJob, t time.Time) {
	cj.Status.LastScheduleTime = t
	err := cc.Client.CronJobs().UpdateStatus(cj.Namespace, cj.Name, cj)
	if err != nil {
		log.Printf("cronjob controller: %v", err)
		return
	}
	expectNewVersion(&cc.expected, cc.Informers.cronJobs(), api.Key(cj.Namespace, cj.Name), cj.Namespace, cj.Name, cj.ResourceVersion)
}

// cleanUp deletes all but the newest few finished Jobs of a CronJob.
func (cc *CronJobController) cleanUp(jobs []api.Job) {
	var succeeded, failed []api.Job
	for _, job := range jobs {
		switch job.Status.Condition {
		case api.JobComplete:
			succeeded = append(succeeded, job)
		case api.JobFailed:
			failed = append(failed, job)
		}
	}

	for _, list := range []struct {
		jobs []api.Job
		keep int
	}{{succeeded, keepSucceededJobs}, {failed, keepFailedJobs}} {
		// Newest first, then delete everything after the first `keep`.
		slices.SortFunc(list.jobs, func(a, b api.Job) int {
			return b.Status.CompletionTime.Compare(a.Status.CompletionTime)
		})
		for _, job := range list.jobs[min(list.keep, len(list.jobs)):] {
			cc.deleteJob(job, "it is older than the history the cronjob keeps")
		}
	}
}

func (cc *CronJobController) deleteJob(job api.Job, reason string) {
	err := cc.Client.Jobs().Delete(job.Namespace, job.Name)
	if err != nil {
		log.Printf("cronjob controller: %v", err)
		return
	}
	log.Printf("deleted job %s: %s", api.Key(job.Namespace, job.Name), reason)
}

// events returns the CronJob controller's event recorder.
func (cc *CronJobController) events() *client.Recorder {
	return cc.Client.Recorder("cronjob-controller")
}
