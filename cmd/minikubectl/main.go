package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

const usage = `usage: minikubectl [-server URL] <command> [-n namespace | -A]

commands:
  get <resource> [-w for pods]
  apply -f <file.json>
  create namespace <name>
  delete <resource> <name>
  scale replicaset|deployment|statefulset <name> <replicas>
  describe <resource> <name>
  logs <pod> [-c container] [-f]
  version

resources: pods, nodes, replicasets, deployments, statefulsets, daemonsets,
jobs, cronjobs, services, configmaps, secrets, events, namespaces

-n picks the namespace (default "default"); -A means every namespace.
Both can go anywhere after the command.`

func main() {
	server := flag.String("server", "http://localhost:8080", "API server address")
	flag.Usage = func() { fmt.Fprintln(os.Stderr, usage) }
	flag.Parse()

	args, namespace, given, all, err := splitNamespaceFlags(flag.Args())
	if err == nil {
		c := &cli{client: client.New(*server), namespace: namespace, namespaceGiven: given, all: all}
		err = c.run(args)
	}
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

// cli holds what every command needs: the API client, and the namespace to
// work in.
type cli struct {
	client         *client.Client
	namespace      string // from -n, or "default"
	namespaceGiven bool   // whether -n was given
	all            bool   // -A: every namespace
}

// listNamespace returns the namespace to list objects in: "" means all of them.
func (c *cli) listNamespace() string {
	if c.all {
		return ""
	}
	return c.namespace
}

// splitNamespaceFlags takes -n/--namespace and -A/--all-namespaces out of
// args, wherever they are, and returns the other arguments. The flag package
// stops at the first argument that isn't a flag, but kubectl users expect
// to write `get pods -n dev`, so we look for these two ourselves.
func splitNamespaceFlags(args []string) (rest []string, namespace string, given, all bool, err error) {
	namespace = api.DefaultNamespace

	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-A" || arg == "--all-namespaces":
			all = true
		case arg == "-n" || arg == "--namespace":
			if i+1 == len(args) {
				return nil, "", false, false, fmt.Errorf("%s needs a namespace after it", arg)
			}
			i++ // the value is the next argument; skip over it
			namespace, given = args[i], true
		case strings.HasPrefix(arg, "-n=") || strings.HasPrefix(arg, "--namespace="):
			_, namespace, _ = strings.Cut(arg, "=")
			given = true
		default:
			rest = append(rest, arg)
		}
	}
	return rest, namespace, given, all, nil
}

// run carries out one command.
func (c *cli) run(args []string) error {
	if len(args) == 0 {
		return errors.New("no command given\n" + usage)
	}

	switch args[0] {
	case "version":
		fmt.Println("minikubectl v0.0.1")
		return nil

	case "get":
		if len(args) < 2 {
			return errors.New("you must specify a resource, e.g. 'get pods'")
		}
		return c.get(args[1], len(args) >= 3 && args[2] == "-w")

	case "apply":
		if len(args) < 3 || args[1] != "-f" {
			return errors.New("usage: minikubectl apply -f <file.json>")
		}
		return c.applyFile(args[2])

	case "create":
		if len(args) < 3 || !isNamespace(args[1]) {
			return errors.New("usage: minikubectl create namespace <name>")
		}
		return c.createNamespace(args[2])

	case "delete":
		if len(args) < 3 {
			return errors.New("usage: minikubectl delete <resource> <name>")
		}
		return c.delete(args[1], args[2])

	case "scale":
		if len(args) < 4 || !(isReplicaSet(args[1]) || isDeployment(args[1]) || isStatefulSet(args[1])) {
			return errors.New("usage: minikubectl scale replicaset|deployment|statefulset <name> <replicas>")
		}
		replicas, err := strconv.Atoi(args[3])
		if err != nil || replicas < 0 {
			return fmt.Errorf("replicas must be a whole number, 0 or more, not %q", args[3])
		}
		return c.scale(args[1], args[2], replicas)

	case "describe":
		if len(args) < 3 {
			return errors.New("usage: minikubectl describe <resource> <name>")
		}
		return c.describe(args[1], args[2])

	case "logs":
		return c.logs(args[1:])

	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
}

// isPod reports whether a resource name on the command line means pods.
func isPod(resource string) bool {
	return resource == "pods" || resource == "pod" || resource == "po"
}

// isNode reports whether a resource name on the command line means nodes.
func isNode(resource string) bool {
	return resource == "nodes" || resource == "node" || resource == "no"
}

// isReplicaSet reports whether a resource name on the command line means ReplicaSets.
func isReplicaSet(resource string) bool {
	return resource == "replicasets" || resource == "replicaset" || resource == "rs"
}

// isDeployment reports whether a resource name on the command line means Deployments.
func isDeployment(resource string) bool {
	return resource == "deployments" || resource == "deployment" || resource == "deploy"
}

// isService reports whether a resource name on the command line means Services.
func isService(resource string) bool {
	return resource == "services" || resource == "service" || resource == "svc"
}

// isNamespace reports whether a resource name on the command line means namespaces.
func isNamespace(resource string) bool {
	return resource == "namespaces" || resource == "namespace" || resource == "ns"
}

// isEvent reports whether a resource name on the command line means events.
func isEvent(resource string) bool {
	return resource == "events" || resource == "event" || resource == "ev"
}
