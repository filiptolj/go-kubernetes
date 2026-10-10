package client

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// ListNamespaces fetches every namespace.
func (c *Client) ListNamespaces() ([]api.Namespace, error) {
	var namespaces []api.Namespace
	err := c.get("/api/v1/namespaces", &namespaces)
	if err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	return namespaces, nil
}

// CreateNamespace creates a namespace.
func (c *Client) CreateNamespace(name string) error {
	ns := api.Namespace{TypeMeta: api.TypeMetaFor("Namespace"), ObjectMeta: api.ObjectMeta{Name: name}}
	err := c.send(http.MethodPost, "/api/v1/namespaces", ns, http.StatusCreated)
	if err != nil {
		return fmt.Errorf("create namespace %q: %w", name, err)
	}
	return nil
}

// DeleteNamespace deletes a namespace and everything in it.
func (c *Client) DeleteNamespace(name string) error {
	err := c.send(http.MethodDelete, "/api/v1/namespaces/"+url.PathEscape(name), nil, http.StatusOK)
	if err != nil {
		return fmt.Errorf("delete namespace %q: %w", name, err)
	}
	return nil
}

// ListReplicaSets fetches the ReplicaSets in a namespace, or in all
// namespaces if namespace is "".
func (c *Client) ListReplicaSets(namespace string) ([]api.ReplicaSet, error) {
	var sets []api.ReplicaSet
	err := c.get(listPath(namespace, "replicasets"), &sets)
	if err != nil {
		return nil, fmt.Errorf("list replicasets: %w", err)
	}
	return sets, nil
}

// CreateReplicaSet sends a new ReplicaSet to the API server, in rs.Namespace.
func (c *Client) CreateReplicaSet(rs api.ReplicaSet) error {
	err := c.send(http.MethodPost, listPath(rs.Namespace, "replicasets"), rs, http.StatusCreated)
	if err != nil {
		return fmt.Errorf("create replicaset: %w", err)
	}
	return nil
}

// UpdateReplicaSet replaces a ReplicaSet.
func (c *Client) UpdateReplicaSet(rs api.ReplicaSet) error {
	err := c.send(http.MethodPut, objectPath(rs.Namespace, "replicasets", rs.Name), rs, http.StatusOK)
	if err != nil {
		return fmt.Errorf("update replicaset %q: %w", rs.Name, err)
	}
	return nil
}

// DeleteReplicaSet removes a ReplicaSet.
func (c *Client) DeleteReplicaSet(namespace, name string) error {
	err := c.send(http.MethodDelete, objectPath(namespace, "replicasets", name), nil, http.StatusOK)
	if err != nil {
		return fmt.Errorf("delete replicaset %q: %w", name, err)
	}
	return nil
}

// ScaleReplicaSet changes how many replicas a ReplicaSet wants.
func (c *Client) ScaleReplicaSet(namespace, name string, replicas int) error {
	err := c.send(http.MethodPost, objectPath(namespace, "replicasets", name)+"/scale", api.Scale{Replicas: replicas}, http.StatusOK)
	if err != nil {
		return fmt.Errorf("scale replicaset %q: %w", name, err)
	}
	return nil
}

// ListDeployments fetches the Deployments in a namespace, or in all
// namespaces if namespace is "".
func (c *Client) ListDeployments(namespace string) ([]api.Deployment, error) {
	var ds []api.Deployment
	err := c.get(listPath(namespace, "deployments"), &ds)
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	return ds, nil
}

// CreateDeployment sends a new Deployment to the API server, in d.Namespace.
func (c *Client) CreateDeployment(d api.Deployment) error {
	err := c.send(http.MethodPost, listPath(d.Namespace, "deployments"), d, http.StatusCreated)
	if err != nil {
		return fmt.Errorf("create deployment: %w", err)
	}
	return nil
}

// UpdateDeployment replaces an existing Deployment.
func (c *Client) UpdateDeployment(d api.Deployment) error {
	err := c.send(http.MethodPut, objectPath(d.Namespace, "deployments", d.Name), d, http.StatusOK)
	if err != nil {
		return fmt.Errorf("update deployment %q: %w", d.Name, err)
	}
	return nil
}

// DeleteDeployment removes a Deployment.
func (c *Client) DeleteDeployment(namespace, name string) error {
	err := c.send(http.MethodDelete, objectPath(namespace, "deployments", name), nil, http.StatusOK)
	if err != nil {
		return fmt.Errorf("delete deployment %q: %w", name, err)
	}
	return nil
}

// ScaleDeployment changes how many replicas a Deployment wants.
func (c *Client) ScaleDeployment(namespace, name string, replicas int) error {
	err := c.send(http.MethodPost, objectPath(namespace, "deployments", name)+"/scale", api.Scale{Replicas: replicas}, http.StatusOK)
	if err != nil {
		return fmt.Errorf("scale deployment %q: %w", name, err)
	}
	return nil
}

// ListServices fetches the Services in a namespace, or in all namespaces if
// namespace is "".
func (c *Client) ListServices(namespace string) ([]api.Service, error) {
	var svcs []api.Service
	err := c.get(listPath(namespace, "services"), &svcs)
	if err != nil {
		return nil, fmt.Errorf("list services: %w", err)
	}
	return svcs, nil
}

// CreateService sends a new Service to the API server, in svc.Namespace.
func (c *Client) CreateService(svc api.Service) error {
	err := c.send(http.MethodPost, listPath(svc.Namespace, "services"), svc, http.StatusCreated)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	return nil
}

// DeleteService removes a Service.
func (c *Client) DeleteService(namespace, name string) error {
	err := c.send(http.MethodDelete, objectPath(namespace, "services", name), nil, http.StatusOK)
	if err != nil {
		return fmt.Errorf("delete service %q: %w", name, err)
	}
	return nil
}
