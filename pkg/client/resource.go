package client

import (
	"fmt"
	"net/http"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// Resource is a client for one kind of namespaced object, such as Jobs. It
// is generic: T is the object's type, and the same code serves every kind.
type Resource[T any] struct {
	c      *Client
	plural string // as in the URL, such as "jobs"
	label  string // for messages, such as "job"
}

// Jobs returns a client for Jobs. The methods below work the same way for
// every kind; they only differ in T and the URL.
func (c *Client) Jobs() *Resource[api.Job] { return &Resource[api.Job]{c, "jobs", "job"} }

// CronJobs returns a client for CronJobs.
func (c *Client) CronJobs() *Resource[api.CronJob] {
	return &Resource[api.CronJob]{c, "cronjobs", "cronjob"}
}

// DaemonSets returns a client for DaemonSets.
func (c *Client) DaemonSets() *Resource[api.DaemonSet] {
	return &Resource[api.DaemonSet]{c, "daemonsets", "daemonset"}
}

// StatefulSets returns a client for StatefulSets.
func (c *Client) StatefulSets() *Resource[api.StatefulSet] {
	return &Resource[api.StatefulSet]{c, "statefulsets", "statefulset"}
}

// ConfigMaps returns a client for ConfigMaps.
func (c *Client) ConfigMaps() *Resource[api.ConfigMap] {
	return &Resource[api.ConfigMap]{c, "configmaps", "configmap"}
}

// Secrets returns a client for Secrets.
func (c *Client) Secrets() *Resource[api.Secret] {
	return &Resource[api.Secret]{c, "secrets", "secret"}
}

// List fetches the objects in a namespace, or in all namespaces if
// namespace is "".
func (r *Resource[T]) List(namespace string) ([]T, error) {
	var objects []T
	err := r.c.get(listPath(namespace, r.plural), &objects)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", r.plural, err)
	}
	return objects, nil
}

// Get fetches one object.
func (r *Resource[T]) Get(namespace, name string) (T, error) {
	var obj T
	err := r.c.get(objectPath(namespace, r.plural, name), &obj)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("get %s %q: %w", r.label, name, err)
	}
	return obj, nil
}

// Create sends a new object to the API server, into namespace.
func (r *Resource[T]) Create(namespace string, obj T) error {
	err := r.c.send(http.MethodPost, listPath(namespace, r.plural), obj, http.StatusCreated)
	if err != nil {
		return fmt.Errorf("create %s: %w", r.label, err)
	}
	return nil
}

// Update replaces an existing object. Its status, if the kind has one, is
// kept: only UpdateStatus changes that.
func (r *Resource[T]) Update(namespace, name string, obj T) error {
	err := r.c.send(http.MethodPut, objectPath(namespace, r.plural, name), obj, http.StatusOK)
	if err != nil {
		return fmt.Errorf("update %s %q: %w", r.label, name, err)
	}
	return nil
}

// UpdateStatus changes only the status of an object. Controllers use it.
func (r *Resource[T]) UpdateStatus(namespace, name string, obj T) error {
	err := r.c.send(http.MethodPut, objectPath(namespace, r.plural, name)+"/status", obj, http.StatusOK)
	if err != nil {
		return fmt.Errorf("update status of %s %q: %w", r.label, name, err)
	}
	return nil
}

// Delete removes an object.
func (r *Resource[T]) Delete(namespace, name string) error {
	err := r.c.send(http.MethodDelete, objectPath(namespace, r.plural, name), nil, http.StatusOK)
	if err != nil {
		return fmt.Errorf("delete %s %q: %w", r.label, name, err)
	}
	return nil
}
