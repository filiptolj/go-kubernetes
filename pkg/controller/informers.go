package controller

import (
	"context"
	"sync"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// Informers are the caches the controllers read from: one per kind, shared
// by every controller that needs it. Reading from them instead of listing
// everything from the API server on every check keeps the load on the API
// server low, and their changes tell the controllers when to look.
//
// A controller without Informers (nil) asks the API server directly. The
// tests do that, so that each check sees every change at once.
type Informers struct {
	Pods         *client.Informer[api.Pod]
	Nodes        *client.Informer[api.Node]
	ReplicaSets  *client.Informer[api.ReplicaSet]
	Deployments  *client.Informer[api.Deployment]
	Jobs         *client.Informer[api.Job]
	CronJobs     *client.Informer[api.CronJob]
	DaemonSets   *client.Informer[api.DaemonSet]
	StatefulSets *client.Informer[api.StatefulSet]
	Autoscalers  *client.Informer[api.HorizontalPodAutoscaler]
}

// NewInformers returns an informer for every kind the controllers use.
func NewInformers(c *client.Client) *Informers {
	return &Informers{
		Pods:         client.NewInformer[api.Pod](c, "pods"),
		Nodes:        client.NewInformer[api.Node](c, "nodes"),
		ReplicaSets:  client.NewInformer[api.ReplicaSet](c, "replicasets"),
		Deployments:  client.NewInformer[api.Deployment](c, "deployments"),
		Jobs:         client.NewInformer[api.Job](c, "jobs"),
		CronJobs:     client.NewInformer[api.CronJob](c, "cronjobs"),
		DaemonSets:   client.NewInformer[api.DaemonSet](c, "daemonsets"),
		StatefulSets: client.NewInformer[api.StatefulSet](c, "statefulsets"),
		Autoscalers:  client.NewInformer[api.HorizontalPodAutoscaler](c, "horizontalpodautoscalers"),
	}
}

// Run runs every informer until ctx is cancelled.
func (inf *Informers) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Go(func() { inf.Pods.Run(ctx) })
	wg.Go(func() { inf.Nodes.Run(ctx) })
	wg.Go(func() { inf.ReplicaSets.Run(ctx) })
	wg.Go(func() { inf.Deployments.Run(ctx) })
	wg.Go(func() { inf.Jobs.Run(ctx) })
	wg.Go(func() { inf.CronJobs.Run(ctx) })
	wg.Go(func() { inf.DaemonSets.Run(ctx) })
	wg.Go(func() { inf.StatefulSets.Run(ctx) })
	wg.Go(func() { inf.Autoscalers.Run(ctx) })
	wg.Wait()
}

// The accessors below return nil when inf is nil, so a controller can ask
// for an informer whether or not it has any.

func (inf *Informers) pods() *client.Informer[api.Pod] {
	if inf == nil {
		return nil
	}
	return inf.Pods
}

func (inf *Informers) nodes() *client.Informer[api.Node] {
	if inf == nil {
		return nil
	}
	return inf.Nodes
}

func (inf *Informers) replicaSets() *client.Informer[api.ReplicaSet] {
	if inf == nil {
		return nil
	}
	return inf.ReplicaSets
}

func (inf *Informers) deployments() *client.Informer[api.Deployment] {
	if inf == nil {
		return nil
	}
	return inf.Deployments
}

func (inf *Informers) jobs() *client.Informer[api.Job] {
	if inf == nil {
		return nil
	}
	return inf.Jobs
}

func (inf *Informers) cronJobs() *client.Informer[api.CronJob] {
	if inf == nil {
		return nil
	}
	return inf.CronJobs
}

func (inf *Informers) daemonSets() *client.Informer[api.DaemonSet] {
	if inf == nil {
		return nil
	}
	return inf.DaemonSets
}

func (inf *Informers) autoscalers() *client.Informer[api.HorizontalPodAutoscaler] {
	if inf == nil {
		return nil
	}
	return inf.Autoscalers
}

func (inf *Informers) statefulSets() *client.Informer[api.StatefulSet] {
	if inf == nil {
		return nil
	}
	return inf.StatefulSets
}

// list returns every object of one kind: from its informer, or straight from
// the API server if there is none.
func list[T any](inf *client.Informer[T], fetch func(namespace string) ([]T, error)) ([]T, error) {
	if inf == nil {
		return fetch("")
	}
	return inf.List(""), nil
}

// allNodes adapts Client.ListNodes for list: nodes don't live in a
// namespace, so it has no namespace argument.
func allNodes(c *client.Client) func(string) ([]api.Node, error) {
	return func(string) ([]api.Node, error) { return c.ListNodes() }
}
