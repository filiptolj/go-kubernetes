package controller

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// JobController runs the pods of every Job until enough of them have
// succeeded, or too many have failed.
type JobController struct {
	Client *client.Client
	Every  time.Duration // how often to check
}

// Run checks the Jobs every jc.Every until ctx is cancelled.
func (jc *JobController) Run(ctx context.Context) {
	runEvery(ctx, jc.Every, jc.reconcileAll)
}

// reconcileAll moves every Job forward, and removes pods whose Job is gone.
func (jc *JobController) reconcileAll() {
	jobs, err := jc.Client.Jobs().List("")
	if err != nil {
		log.Printf("job controller: %v", err)
		return
	}
	pods, err := jc.Client.ListPods("")
	if err != nil {
		log.Printf("job controller: %v", err)
		return
	}

	owned := groupByOwner(pods, "Job")
	exists := make(map[string]bool)
	for _, job := range jobs {
		key := api.Key(job.Namespace, job.Name)
		exists[key] = true
		jc.reconcile(job, owned[key])
	}

	for owner, pods := range owned {
		if exists[owner] {
			continue
		}
		for _, pod := range pods {
			jc.deletePod(pod, fmt.Sprintf("its job %q is gone", pod.Owner))
		}
	}
}

// reconcile counts a Job's pods, decides whether it has finished, starts
// more pods if it hasn't, and saves its status.
func (jc *JobController) reconcile(job api.Job, pods []api.Pod) {
	var active []api.Pod
	succeeded, failed := 0, 0
	for _, pod := range pods {
		switch pod.Phase {
		case api.PodSucceeded:
			succeeded++
		case api.PodFailed:
			failed++
		default:
			active = append(active, pod)
		}
		// With restartPolicy OnFailure, a failing pod isn't replaced but
		// restarted in place by the kubelet. Those restarts count as failures too.
		failed += pod.Restarts
	}

	backoffLimit := 6
	if job.BackoffLimit != nil {
		backoffLimit = *job.BackoffLimit
	}

	status := job.Status
	status.Active, status.Succeeded, status.Failed = len(active), succeeded, failed
	if status.StartTime.IsZero() {
		status.StartTime = time.Now()
	}

	if status.Condition == "" {
		switch {
		case succeeded >= job.Completions:
			status.Condition, status.CompletionTime = api.JobComplete, time.Now()
			jc.events().Normal("Job", job.Namespace, job.Name, "Completed", "%d of %d pods succeeded", succeeded, job.Completions)
		case failed > backoffLimit:
			status.Condition, status.CompletionTime = api.JobFailed, time.Now()
			jc.events().Warning("Job", job.Namespace, job.Name, "BackoffLimitExceeded",
				"%d failures, more than the backoff limit of %d", failed, backoffLimit)
		}
	}

	if status.Condition != "" {
		// A finished Job runs nothing more. Its finished pods are kept, so
		// their logs can still be read; they go when the Job is deleted.
		for _, pod := range active {
			jc.deletePod(pod, "its job has finished")
		}
		status.Active = 0
	} else {
		// Run as many pods at once as parallelism allows, but no more than
		// are still needed.
		missing := min(job.Parallelism, job.Completions-succeeded) - len(active)
		for range missing {
			pod := newPod(newPodName(job.Name), job.Namespace, job.Template, "Job", job.Name)
			err := jc.Client.CreatePod(pod)
			if err != nil {
				log.Printf("job %s: %v", api.Key(job.Namespace, job.Name), err)
				continue
			}
			status.Active++
			jc.events().Normal("Job", job.Namespace, job.Name, "SuccessfulCreate", "created pod %q", pod.Name)
		}
	}

	if status != job.Status {
		job.Status = status
		err := jc.Client.Jobs().UpdateStatus(job.Namespace, job.Name, job)
		if err != nil {
			log.Printf("job controller: %v", err)
		}
	}
}

func (jc *JobController) deletePod(pod api.Pod, reason string) {
	err := jc.Client.DeletePod(pod.Namespace, pod.Name)
	if err != nil {
		log.Printf("job controller: %v", err)
		return
	}
	log.Printf("deleted pod %s: %s", api.Key(pod.Namespace, pod.Name), reason)
}

// events returns the Job controller's event recorder.
func (jc *JobController) events() *client.Recorder {
	return jc.Client.Recorder("job-controller")
}

// runEvery calls fn every interval until ctx is cancelled.
func runEvery(ctx context.Context, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}
