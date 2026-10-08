package store

import (
	"fmt"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// CreateDeployment saves a new Deployment. It fails if one with that name
// already exists in its namespace.
func (s *Store) CreateDeployment(d api.Deployment) (api.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.checkNamespace(d.Namespace)
	if err != nil {
		return api.Deployment{}, err
	}

	key := api.Key(d.Namespace, d.Name)
	_, exists := s.deployments[key]
	if exists {
		return api.Deployment{}, fmt.Errorf("deployment %q already exists in namespace %q: %w", d.Name, d.Namespace, ErrConflict)
	}

	s.stampNew(&d.ObjectMeta)
	err = s.put(api.EventAdded, kindDeployments, key, d)
	if err != nil {
		return api.Deployment{}, err
	}
	s.deployments[key] = d
	return d, nil
}

// UpdateDeployment replaces an existing Deployment. Changing its template
// starts a rolling update.
func (s *Store) UpdateDeployment(d api.Deployment) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := api.Key(d.Namespace, d.Name)
	stored, ok := s.deployments[key]
	if !ok {
		return fmt.Errorf("deployment %q in namespace %q: %w", d.Name, d.Namespace, ErrNotFound)
	}

	err := s.stampUpdate(&d.ObjectMeta, stored.ObjectMeta)
	if err != nil {
		return fmt.Errorf("deployment %q: %w", d.Name, err)
	}
	err = s.put(api.EventModified, kindDeployments, key, d)
	if err != nil {
		return err
	}
	s.deployments[key] = d
	return nil
}

// ListDeployments returns the Deployments in a namespace, or in all
// namespaces if namespace is "".
func (s *Store) ListDeployments(namespace string) []api.Deployment {
	s.mu.Lock()
	defer s.mu.Unlock()

	return inNamespace(s.deployments, namespace)
}

// DeleteDeployment removes a Deployment. Its ReplicaSets are left behind for
// the Deployment controller to clean up.
func (s *Store) DeleteDeployment(namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := api.Key(namespace, name)
	_, ok := s.deployments[key]
	if !ok {
		return fmt.Errorf("deployment %q in namespace %q: %w", name, namespace, ErrNotFound)
	}

	err := s.remove(kindDeployments, key, s.deployments[key])
	if err != nil {
		return err
	}
	delete(s.deployments, key)
	return nil
}

// ScaleDeployment changes how many replicas a Deployment wants.
func (s *Store) ScaleDeployment(namespace, name string, replicas int) (api.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := api.Key(namespace, name)
	d, ok := s.deployments[key]
	if !ok {
		return api.Deployment{}, fmt.Errorf("deployment %q in namespace %q: %w", name, namespace, ErrNotFound)
	}

	d.Replicas = replicas
	d.ResourceVersion = s.nextVersion()
	err := s.put(api.EventModified, kindDeployments, key, d)
	if err != nil {
		return api.Deployment{}, err
	}
	s.deployments[key] = d
	return d, nil
}

// CreateService saves a new Service. It fails if one with that name already
// exists in its namespace, or if any Service, in any namespace, already uses
// its port: the proxy listens on one port per Service for the whole cluster.
func (s *Store) CreateService(svc api.Service) (api.Service, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.checkNamespace(svc.Namespace)
	if err != nil {
		return api.Service{}, err
	}

	key := api.Key(svc.Namespace, svc.Name)
	_, exists := s.services[key]
	if exists {
		return api.Service{}, fmt.Errorf("service %q already exists in namespace %q: %w", svc.Name, svc.Namespace, ErrConflict)
	}
	for _, other := range s.services {
		for _, theirs := range other.Ports {
			for _, ours := range svc.Ports {
				if theirs.Port == ours.Port {
					return api.Service{}, fmt.Errorf("port %d is already used by service %q in namespace %q: %w",
						ours.Port, other.Name, other.Namespace, ErrConflict)
				}
			}
		}
	}

	s.stampNew(&svc.ObjectMeta)
	err = s.put(api.EventAdded, kindServices, key, svc)
	if err != nil {
		return api.Service{}, err
	}
	s.services[key] = svc
	return svc, nil
}

// ListServices returns the Services in a namespace, or in all namespaces if
// namespace is "".
func (s *Store) ListServices(namespace string) []api.Service {
	s.mu.Lock()
	defer s.mu.Unlock()

	return inNamespace(s.services, namespace)
}

// DeleteService removes a Service.
func (s *Store) DeleteService(namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := api.Key(namespace, name)
	_, ok := s.services[key]
	if !ok {
		return fmt.Errorf("service %q in namespace %q: %w", name, namespace, ErrNotFound)
	}

	err := s.remove(kindServices, key, s.services[key])
	if err != nil {
		return err
	}
	delete(s.services, key)
	return nil
}
