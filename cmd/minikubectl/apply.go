package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// applyPath applies a manifest file, or every manifest (.yaml, .yml or
// .json) in a folder.
func (c *cli) applyPath(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return c.applyFile(path)
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		ext := filepath.Ext(entry.Name())
		if entry.IsDir() || (ext != ".yaml" && ext != ".yml" && ext != ".json") {
			continue
		}
		err := c.applyFile(filepath.Join(path, entry.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

// applyFile applies every object in a manifest file. The file is YAML or
// JSON, and may hold several objects separated by lines of "---".
func (c *cli) applyFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	for i, doc := range splitDocuments(data) {
		// YAMLToJSON turns YAML into JSON, so the JSON tags on our types
		// work for both. JSON is valid YAML, so .json files go through too.
		jsonData, err := yaml.YAMLToJSON(doc)
		if err != nil {
			return fmt.Errorf("%s, object %d: %w", path, i+1, err)
		}
		err = c.applyObject(jsonData)
		if err != nil {
			return fmt.Errorf("%s, object %d: %w", path, i+1, err)
		}
	}
	return nil
}

// splitDocuments splits a YAML file at lines of "---", dropping documents
// that are empty or only hold comments.
func splitDocuments(data []byte) [][]byte {
	var docs [][]byte
	var current []string
	flush := func() {
		text := strings.Join(current, "\n")
		for _, line := range current {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
				docs = append(docs, []byte(text))
				break
			}
		}
		current = nil
	}

	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimRight(line, " \r") == "---" {
			flush()
			continue
		}
		current = append(current, line)
	}
	flush()
	return docs
}

// applyObject creates one object, given as JSON, on the API server. Its
// "kind" says what it is. An object that lives in a namespace goes in the
// namespace its metadata names, or else the one from -n.
func (c *cli) applyObject(data []byte) error {
	var header struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Namespace string `json:"namespace"`
		} `json:"metadata"`
	}
	err := json.Unmarshal(data, &header)
	if err != nil {
		return err
	}

	namespace, err := c.namespaceFor(header.Metadata.Namespace)
	if err != nil {
		return err
	}

	ok, err := c.applyWorkload(header.Kind, data, namespace)
	if ok {
		return err
	}

	switch header.Kind {
	case "Pod":
		var pod api.Pod
		err = json.Unmarshal(data, &pod)
		if err != nil {
			return err
		}
		pod.Namespace = namespace
		return created("pod", pod.Name, namespace, c.client.CreatePod(pod))

	case "ReplicaSet":
		var rs api.ReplicaSet
		err = json.Unmarshal(data, &rs)
		if err != nil {
			return err
		}
		rs.Namespace = namespace
		return created("replicaset", rs.Name, namespace, c.client.CreateReplicaSet(rs))

	case "Deployment":
		var d api.Deployment
		err = json.Unmarshal(data, &d)
		if err != nil {
			return err
		}
		d.Namespace = namespace
		return c.applyDeployment(d)

	case "Service":
		var svc api.Service
		err = json.Unmarshal(data, &svc)
		if err != nil {
			return err
		}
		svc.Namespace = namespace
		return created("service", svc.Name, namespace, c.client.CreateService(svc))

	case "Node":
		var node api.Node
		err = json.Unmarshal(data, &node)
		if err != nil {
			return err
		}
		return created("node", node.Name, "", c.client.CreateNode(node))

	case "Namespace":
		var ns api.Namespace
		err = json.Unmarshal(data, &ns)
		if err != nil {
			return err
		}
		return c.createNamespace(ns.Name)

	case "":
		return errors.New(`missing "kind", such as "Pod" or "Deployment"`)
	default:
		return fmt.Errorf(`unknown "kind": %s`, header.Kind)
	}
}

// created reports the result of creating an object that apply can't update.
// If it already exists, it is left as it is, so that applying the same files
// again works.
func created(label, name, namespace string, err error) error {
	where := ""
	if namespace != "" {
		where = " in namespace " + namespace
	}
	switch {
	case errors.Is(err, client.ErrConflict):
		fmt.Printf("%s/%s already exists%s, left as it is (delete it first to change it)\n", label, name, where)
		return nil
	case err != nil:
		return err
	}
	fmt.Printf("%s/%s created%s\n", label, name, where)
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
//
// A running pod is given grace seconds to stop (below 0: its own grace
// period; 0: none), and is Terminating meanwhile.
func (c *cli) delete(resource, name string, grace int) error {
	var err error
	switch {
	case isPod(resource):
		err = c.client.DeletePodWithGrace(c.namespace, name, grace)
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
