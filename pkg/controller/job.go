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
	Client    *client.Client
	Informers *Informers    // where to read from; nil: from Client
	Every     time.Duration // how often to check even if nothing seems to change

	expected expectations // pods created or deleted but not seen yet, by Job
}

// Run checks the Jobs whenever a Job or a pod changes, and every jc.Every,
// until ctx is cancelled.
func (jc *JobController) Run(ctx context.Context) {
	client.RunOnChange(ctx, "job controller", jc.Every, jc.reconcileAll, jc.Informers.jobs(), jc.Informers.pods())
}

// reconcileAll moves every Job forward, and removes pods whose Job is gone.
func (jc *JobController) reconcileAll() {
	jc.expected.check() // before reading: see expectations.go
	jobs, err := list(jc.Informers.jobs(), jc.Client.Jobs().List)
	if err != nil {
		log.Printf("job controller: %v", err)
		return
	}
	pods, err := list(jc.Informers.pods(), jc.Client.ListPods)
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
			jc.deletePod(pod, fmt.Sprintf("its job %q is gone", pod.OwnerName()))
		}
	}
}

// reconcile counts a Job's pods, decides whether it has finished, starts
// more pods if it hasn't, and saves its status.
func (jc *JobController) reconcile(job api.Job, pods []api.Pod) {
	if !jc.expected.satisfied(api.Key(job.Namespace, job.Name)) {
		return // the pods may not show our last changes yet
	}

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
			pod := newPod(newPodName(job.Name), job.Template, "Job", job.ObjectMeta)
			err := jc.Client.CreatePod(pod)
			if err != nil {
				log.Printf("job %s: %v", api.Key(job.Namespace, job.Name), err)
				continue
			}
			expectPresent(&jc.expected, jc.Informers.pods(), api.Key(job.Namespace, job.Name), pod.Namespace, pod.Name)
			status.Active++
			jc.events().Normal("Job", job.Namespace, job.Name, "SuccessfulCreate", "created pod %q", pod.Name)
		}
	}

	if status != job.Status {
		job.Status = status
		err := jc.Client.Jobs().UpdateStatus(job.Namespace, job.Name, job)
		if err != nil {
			log.Printf("job controller: %v", err)
			return
		}
		expectNewVersion(&jc.expected, jc.Informers.jobs(), api.Key(job.Namespace, job.Name), job.Namespace, job.Name, job.ResourceVersion)
	}
}

func (jc *JobController) deletePod(pod api.Pod, reason string) {
	err := jc.Client.DeletePod(pod.Namespace, pod.Name)
	if err != nil {
		log.Printf("job controller: %v", err)
		return
	}
	expectGone(&jc.expected, jc.Informers.pods(), api.Key(pod.Namespace, pod.OwnerName()), pod.Namespace, pod.Name)
	log.Printf("deleted pod %s: %s", api.Key(pod.Namespace, pod.Name), reason)
}

// events returns the Job controller's event recorder.
func (jc *JobController) events() *client.Recorder {
	return jc.Client.Recorder("job-controller")
}
