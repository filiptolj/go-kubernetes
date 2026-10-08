package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// applyFile reads an object from a JSON file and creates it on the API
// server. The file's "kind" field says what it is. An object that lives in a
// namespace goes in the namespace the file names, or else the one from -n.
func (c *cli) applyFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var header struct {
		Kind      string `json:"kind"`
		Namespace string `json:"namespace"`
	}
	err = json.Unmarshal(data, &header)
	if err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}

	namespace, err := c.namespaceFor(header.Namespace)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	ok, err := c.applyWorkload(header.Kind, data, namespace)
	if ok {
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		return nil
	}

	switch header.Kind {
	case "Pod":
		var pod api.Pod
		err = json.Unmarshal(data, &pod)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		pod.Namespace = namespace
		err = c.client.CreatePod(pod)
		if err != nil {
			return err
		}
		fmt.Printf("pod/%s created in namespace %s\n", pod.Name, namespace)

	case "ReplicaSet":
		var rs api.ReplicaSet
		err = json.Unmarshal(data, &rs)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		rs.Namespace = namespace
		err = c.client.CreateReplicaSet(rs)
		if err != nil {
			return err
		}
		fmt.Printf("replicaset/%s created in namespace %s\n", rs.Name, namespace)

	case "Deployment":
		var d api.Deployment
		err = json.Unmarshal(data, &d)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		d.Namespace = namespace
		return c.applyDeployment(d)

	case "Service":
		var svc api.Service
		err = json.Unmarshal(data, &svc)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		svc.Namespace = namespace
		err = c.client.CreateService(svc)
		if err != nil {
			return err
		}
		fmt.Printf("service/%s created in namespace %s\n", svc.Name, namespace)

	case "Node":
		var node api.Node
		err = json.Unmarshal(data, &node)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		err = c.client.CreateNode(node)
		if err != nil {
			return err
		}
		fmt.Printf("node/%s created\n", node.Name)

	case "Namespace":
		var ns api.Namespace
		err = json.Unmarshal(data, &ns)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		return c.createNamespace(ns.Name)

	case "":
		return fmt.Errorf("%s: missing \"kind\", such as \"Pod\" or \"Deployment\"", path)
	default:
		return fmt.Errorf("%s: unknown \"kind\": %s", path, header.Kind)
	}
	return nil
}

// namespaceFor picks the namespace for an object read from a file: the one
// the file names, or else the one from -n. A file that names one namespace,
// used with -n for another, is refused: that's almost certainly a mistake.
func (c *cli) namespaceFor(fromFile string) (string, error) {
	switch {
	case fromFile == "":
		return c.namespace, nil
	case c.namespaceGiven && fromFile != c.namespace:
		return "", fmt.Errorf("the file says namespace %q, but -n says %q", fromFile, c.namespace)
	default:
		return fromFile, nil
	}
}

// applyDeployment creates a Deployment, or updates it if it already exists.
// Updating one with a new template starts a rolling update.
func (c *cli) applyDeployment(d api.Deployment) error {
	err := c.client.CreateDeployment(d)
	if err == nil {
		fmt.Printf("deployment/%s created in namespace %s\n", d.Name, d.Namespace)
		return nil
	}
	if !errors.Is(err, client.ErrConflict) {
		return err
	}

	err = c.client.UpdateDeployment(d)
	if err != nil {
		return err
	}
	fmt.Printf("deployment/%s configured in namespace %s\n", d.Name, d.Namespace)
	return nil
}

// createNamespace creates a namespace. One that already exists is left alone.
func (c *cli) createNamespace(name string) error {
	err := c.client.CreateNamespace(name)
	if errors.Is(err, client.ErrConflict) {
		fmt.Printf("namespace/%s already exists\n", name)
		return nil
	}
	if err != nil {
		return err
	}
	fmt.Printf("namespace/%s created\n", name)
	return nil
}

// delete deletes one object by name.
func (c *cli) delete(resource, name string) error {
	var err error
	switch {
	case isPod(resource):
		err = c.client.DeletePod(c.namespace, name)
		resource = "pod"
	case isReplicaSet(resource):
		err = c.client.DeleteReplicaSet(c.namespace, name)
		resource = "replicaset"
	case isDeployment(resource):
		err = c.client.DeleteDeployment(c.namespace, name)
		resource = "deployment"
	case isService(resource):
		err = c.client.DeleteService(c.namespace, name)
		resource = "service"
	case isNamespace(resource):
		err = c.client.DeleteNamespace(name)
		if err == nil {
			fmt.Printf("namespace/%s deleted, with everything in it\n", name)
		}
		return err
	default:
		ok, err := c.deleteWorkload(resource, name)
		if !ok {
			return fmt.Errorf("can't delete %q", resource)
		}
		return err
	}

	if err != nil {
		return err
	}
	fmt.Printf("%s/%s deleted from namespace %s\n", resource, name, c.namespace)
	return nil
}

// scale changes the number of replicas of a ReplicaSet, Deployment or StatefulSet.
func (c *cli) scale(resource, name string, replicas int) error {
	if isStatefulSet(resource) {
		ss, err := c.client.StatefulSets().Get(c.namespace, name)
		if err != nil {
			return err
		}
		ss.Replicas = replicas
		err = c.client.StatefulSets().Update(c.namespace, name, ss)
		if err != nil {
			return err
		}
		fmt.Printf("statefulset/%s scaled to %d\n", name, replicas)
		return nil
	}

	if isDeployment(resource) {
		err := c.client.ScaleDeployment(c.namespace, name, replicas)
		if err != nil {
			return err
		}
		fmt.Printf("deployment/%s scaled to %d\n", name, replicas)
		return nil
	}

	err := c.client.ScaleReplicaSet(c.namespace, name, replicas)
	if err != nil {
		return err
	}
	fmt.Printf("replicaset/%s scaled to %d\n", name, replicas)
	return nil
}
