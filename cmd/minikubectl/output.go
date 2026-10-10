package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// getCommand reads the arguments of get:
//
//	get <resource> [name] [-l selector] [-o yaml|json|name] [-w]
//
// The flags may come before or after the name.
func (c *cli) getCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("you must specify a resource, e.g. 'get pods'")
	}
	resource := args[0]

	var watch bool
	var output string
	for i := 1; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-w" || arg == "--watch":
			watch = true
		case arg == "-l" || arg == "--selector" || arg == "-o" || arg == "--output":
			if i+1 == len(args) {
				return fmt.Errorf("%s needs a value after it", arg)
			}
			i++
			if strings.HasPrefix(arg, "-l") || arg == "--selector" {
				c.selector = args[i]
			} else {
				output = args[i]
			}
		case strings.HasPrefix(arg, "-l="), strings.HasPrefix(arg, "--selector="):
			_, c.selector, _ = strings.Cut(arg, "=")
		case strings.HasPrefix(arg, "-o="), strings.HasPrefix(arg, "--output="):
			_, output, _ = strings.Cut(arg, "=")
		case strings.HasPrefix(arg, "-"):
			return fmt.Errorf("unknown flag %q for get", arg)
		case c.name == "":
			c.name = arg
		default:
			return fmt.Errorf("get takes one name, not %q and %q", c.name, arg)
		}
	}

	// Check the selector here, so a mistake gets a clear message.
	if _, err := api.ParseSelector(c.selector); err != nil {
		return err
	}
	if watch && !isPod(resource) {
		return errors.New("-w only works for pods")
	}

	if output != "" {
		return c.printObjects(resource, output)
	}
	return c.get(resource, watch)
}

// pluralFor returns the plural of a resource name from the command line, as
// it appears in API URLs: "po", "pod" and "pods" are all "pods".
func pluralFor(resource string) (string, bool) {
	for _, k := range []struct {
		is     func(string) bool
		plural string
	}{
		{isPod, "pods"}, {isNode, "nodes"}, {isReplicaSet, "replicasets"}, {isDeployment, "deployments"},
		{isService, "services"}, {isNamespace, "namespaces"}, {isJob, "jobs"}, {isCronJob, "cronjobs"},
		{isDaemonSet, "daemonsets"}, {isStatefulSet, "statefulsets"}, {isConfigMap, "configmaps"},
		{isSecret, "secrets"}, {isIngress, "ingresses"}, {isAutoscaler, "horizontalpodautoscalers"}, {isLease, "leases"},
	} {
		if k.is(resource) {
			return k.plural, true
		}
	}
	return "", false
}

// clusterScoped reports whether objects of a kind belong to the whole
// cluster rather than to a namespace.
func clusterScoped(plural string) bool {
	return plural == "nodes" || plural == "namespaces"
}

// list fetches the objects of one kind that a get command asks for: in the
// namespace from -n (or all of them with -A), matching -l, and only the one
// named, if a name was given.
func list[T any](c *cli, plural string) ([]T, error) {
	namespace := c.listNamespace()
	if clusterScoped(plural) {
		namespace = ""
	}

	objects, err := client.List[T](c.client, plural, namespace, c.selector)
	if err != nil || c.name == "" {
		return objects, err
	}

	objects = slices.DeleteFunc(objects, func(obj T) bool { return api.MetaOf(&obj).Name != c.name })
	if len(objects) == 0 {
		return nil, fmt.Errorf("%s %q not found", strings.TrimSuffix(plural, "s"), c.name)
	}
	return objects, nil
}

// object is any object, read as plain JSON, for printing. Its keys come out
// sorted, which happens to give the usual order: apiVersion, kind, metadata,
// spec, status.
type object map[string]any

// GetObjectMeta lets list filter plain objects by name, like typed ones.
func (o *object) GetObjectMeta() *api.ObjectMeta {
	var m api.ObjectMeta
	data, _ := json.Marshal((*o)["metadata"])
	json.Unmarshal(data, &m)
	return &m
}

// printObjects prints the objects a get command asks for, in full, as YAML
// or JSON, or only their names. One object is printed as itself; several
// are wrapped in a List, as kubectl does.
func (c *cli) printObjects(resource, output string) error {
	plural, ok := pluralFor(resource)
	if !ok {
		return fmt.Errorf("can't print %q with -o", resource)
	}
	if output != "yaml" && output != "json" && output != "name" {
		return fmt.Errorf("unknown output %q: use yaml, json or name", output)
	}

	objects, err := list[object](c, plural)
	if err != nil {
		return err
	}

	if output == "name" {
		for _, obj := range objects {
			fmt.Println(strings.TrimSuffix(plural, "s") + "/" + obj.GetObjectMeta().Name)
		}
		return nil
	}

	var v any = map[string]any{"apiVersion": "v1", "kind": "List", "items": objects}
	if c.name != "" {
		v = objects[0]
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if output == "yaml" {
		data, err = yaml.JSONToYAML(data)
		if err != nil {
			return err
		}
	} else {
		data = append(data, '\n')
	}
	_, err = os.Stdout.Write(data)
	return err
}
