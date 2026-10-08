# go-kubernetes

A small Kubernetes, written from scratch in Go.

It has the same moving parts as the real thing: an API server backed by etcd,
a scheduler, controllers, a kubelet on every node that runs real Docker
containers, and a proxy that makes Services reachable. Pods, Deployments with
zero-downtime rolling updates, StatefulSets, DaemonSets, Jobs, CronJobs,
Services, ConfigMaps, Secrets, volumes, probes, namespaces, events and a
`kubectl`-style CLI all work. Apart from the etcd client, it uses only Go's
standard library.

It was built as a learning project: to understand how Kubernetes works by
building it, and to learn Go along the way. It is not meant for production.

```
$ minikubectl apply -f examples/web-deployment.yaml
deployment/web created in namespace default

$ minikubectl get pods
NAME                 STATUS    READY   RESTARTS   NODE     ADDRESS
web-5e5373a2-fvz8g   Running   yes     0          node-1   127.0.0.1:33339
web-5e5373a2-ii3sb   Running   yes     0          node-2   127.0.0.1:42105
web-5e5373a2-ue59o   Running   yes     0          node-1   127.0.0.1:39169

$ minikubectl describe pod web-5e5373a2-fvz8g
...
Events:
  TYPE     REASON      AGE   FROM                MESSAGE
  Normal   Scheduled   10s   scheduler           assigned to node "node-1"
  Normal   Started     10s   kubelet on node-1   started container "nginx" from image "nginx:1.27"
  Normal   Ready       8s    kubelet on node-1   answering on 127.0.0.1:33339
```

## Features

- **Pods** that run as real Docker containers, or as plain processes
- **Restart policies**: crashed containers are restarted in place, waiting longer each time (`CrashLoopBackOff`)
- **Liveness and readiness probes**, by HTTP request or by checking the port
- **Scheduling** of pods onto nodes, with a choice of strategies (least loaded, round robin)
- **ReplicaSets** and **Deployments**, with rolling updates that keep the app available the whole time
- **StatefulSets**: numbered pods (`db-0`, `db-1`, ...) created in order, each once the one before is ready
- **DaemonSets**: one pod on every node
- **Jobs** that run pods until enough succeed, and **CronJobs** that create Jobs on a schedule
- **ConfigMaps** and **Secrets**, given to containers as environment variables or files
- **Volumes**: `emptyDir` shared by a pod's containers, `hostPath`, and ConfigMaps and Secrets as files
- **Services** that give a group of pods one address, with load balancing across the ready ones
- **Self-healing**: a node that stops sending heartbeats is marked NotReady and its pods are replaced elsewhere
- **Namespaces**, so the same names can be used by different teams or apps
- **Events** that record what happened to each object, shown by `minikubectl describe`
- **Logs** of any container, also followed live with `-f`
- **Storage** in etcd, or in a JSON file; every component reconnects if the API server restarts
- **Tests** with the race detector, including a fake container runtime and an in-memory API server

## Requirements

- **Linux**, or Windows with **WSL 2**. The kubelet uses Linux process groups.
- **Go 1.26** or newer. With Go 1.21 or newer installed, Go downloads 1.26 by itself the first time you build.
- **Docker**, to run pods as containers. Without it, use `-runtime process` (see below).
- **etcd**, only if you want to store the cluster in etcd instead of a file.

## Quick start

```bash
git clone https://github.com/filiptolj/go-kubernetes.git
cd go-kubernetes

go build -o bin/ ./cmd/...
./bin/minik8s
```

`minik8s` starts the whole cluster in one terminal: the API server, the
controller manager, the scheduler, the proxy and two nodes. Their logs are
shown together, each line marked with the component it came from. Press
Ctrl+C to stop everything; the nodes remove their containers on the way out.

Each node keeps its containers' logs in `/tmp/minik8s/<node>` and its pods'
volumes in `/tmp/minik8s/volumes/<node>`; both are removed when the pod is
deleted. The cluster itself is saved in `data/apiserver.json`.

In a second terminal:

```bash
./bin/minikubectl apply -f examples/web-deployment.yaml   # 3 nginx pods
./bin/minikubectl apply -f examples/web-service.yaml      # one address for them, on port 8081
./bin/minikubectl get pods -w                             # watch them start (Ctrl+C to stop watching)

curl http://localhost:8081                                # "Welcome to nginx!", from one of the pods
```

Try a rolling update: change `nginx:1.27` to `nginx:1.28` in
`examples/web-deployment.yaml` and apply it again. The pods are replaced one at
a time, and `curl` keeps working throughout.

### More examples

Each file in [`examples/`](examples) shows one feature. Apply it, then look
with `get`, `describe` and `logs`:

| File | Shows |
|---|---|
| `job.yaml` | a Job: 3 pods must succeed, 2 run at a time |
| `cronjob.yaml` | a CronJob that runs a Job every minute |
| `statefulset.yaml` | `db-0`, `db-1`, `db-2`, created one after the other |
| `daemonset.yaml` | one pod per node, printing its node's name |
| `configmap.yaml`, `secret.yaml`, `configured-pod.yaml` | settings and a password as environment variables and files (apply in this order) |
| `shared-volume.yaml` | two containers sharing an `emptyDir`: one writes a web page, nginx serves it |
| `liveness.yaml` | nginx with a liveness probe on a page that doesn't exist: watch it get restarted |
| `crash.yaml`, `hello.yaml` | pods with `restartPolicy: Never`, which fail or finish once |
| `web-rs.yaml`, `flaky-rs.yaml` | ReplicaSets, one of them crashing every 10 seconds |

`minik8s` takes a few options:

| Flag | Default | Meaning |
|---|---|---|
| `-nodes` | `2` | how many nodes to run |
| `-runtime` | `docker` | `process` runs each container's `command` as a plain process, without Docker |
| `-etcd` | | store the cluster in etcd at this address, such as `localhost:2379` |
| `-data` | `data/apiserver.json` | file to store the cluster in when not using etcd; `""` keeps it in memory only |
| `-port` | `8080` | the API server's port |
| `-kubelet-port` | `10250` | the first node's port for serving logs; the next nodes use the ports after it |

### Using etcd

```bash
docker run -d --name minik8s-etcd -p 127.0.0.1:2379:2379 \
  gcr.io/etcd-development/etcd:v3.6.5 /usr/local/bin/etcd \
  --data-dir /etcd-data \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://127.0.0.1:2379

./bin/minik8s -etcd localhost:2379
```

Every object is stored under its own key:

```bash
$ docker exec minik8s-etcd etcdctl get --prefix /minik8s/ --keys-only
/minik8s/deployments/default/web
/minik8s/namespaces/default
/minik8s/nodes/node-1
/minik8s/pods/default/web-5e5373a2-fvz8g
...
```

## minikubectl

| Command | What it does |
|---|---|
| `get <resource>` | list objects; `get pods -w` keeps watching for changes |
| `describe <resource> <name>` | details and recent events |
| `apply -f <file.yaml>` | create the object in a file; applying it again updates it |
| `delete <resource> <name>` | delete an object; a namespace is deleted with everything in it |
| `logs <pod> [-c container] [-f]` | a container's output; `-f` follows it |
| `scale replicaset\|deployment\|statefulset <name> <n>` | change the number of replicas |
| `create namespace <name>` | create a namespace |

The resources are `pods`, `nodes`, `replicasets`, `deployments`,
`statefulsets`, `daemonsets`, `jobs`, `cronjobs`, `services`, `configmaps`,
`secrets`, `events` and `namespaces`.

`-n <namespace>` chooses the namespace (the default is `default`), and `-A`
shows every namespace. Both can go anywhere on the command line.
`-server <url>` points `minikubectl` at an API server other than
`http://localhost:8080`; it must come right after `minikubectl`.
Short names work too: `po`, `no`, `rs`, `deploy`, `sts`, `ds`, `cj`, `svc`,
`cm`, `ns`, `ev`.

## Writing objects

Objects are JSON files with a `kind`. These are in [`examples/`](examples):

```json
{
  "kind": "Deployment",
  "name": "web",
  "replicas": 3,
  "template": {
    "labels": { "app": "web" },
    "containers": [
      { "name": "nginx", "image": "nginx:1.27", "port": 80 }
    ]
  }
}
```

```json
{
  "kind": "Service",
  "name": "web",
  "port": 8081,
  "selector": { "app": "web" }
}
```

| Kind | Fields |
|---|---|
| `Pod` | `name`, `labels`, `containers`, `volumes`, `restartPolicy` |
| `ReplicaSet` | `name`, `replicas`, `template` (`labels`, `containers`, `volumes`) |
| `Deployment` | `name`, `replicas`, `template` (`labels` are required) |
| `StatefulSet` | `name`, `replicas`, `template` |
| `DaemonSet` | `name`, `template` |
| `Job` | `name`, `completions` (default 1), `parallelism` (default 1), `backoffLimit` (default 6), `template` |
| `CronJob` | `name`, `schedule` (cron format, such as `*/5 * * * *`), `suspend`, `jobTemplate` (the Job's fields) |
| `Service` | `name`, `port`, `selector` |
| `ConfigMap`, `Secret` | `name`, `data` (keys and values) |
| `Namespace` | `name` |

Every object except Nodes and Namespaces can also name its `namespace`;
otherwise `-n` decides, or `default`.

### Pods and containers

A container has a `name` and an `image`, and optionally:

| Field | Meaning |
|---|---|
| `command` | the command to run, such as `["sh", "-c", "echo hi"]` |
| `port` | the port it listens on; the kubelet makes it reachable and reports it as the pod's address |
| `env` | environment variables: `{"name": "MODE", "value": "fast"}`, or a value from a ConfigMap or Secret: `{"name": "PASSWORD", "valueFrom": {"secretKeyRef": {"name": "db", "key": "password"}}}` |
| `volumeMounts` | where the pod's volumes appear: `{"name": "config", "mountPath": "/etc/app"}` |
| `livenessProbe` | a check that restarts the container when it fails: `{"httpGet": {"path": "/healthz"}, "periodSeconds": 10, "failureThreshold": 3}`; without `httpGet` it checks that the port answers |
| `readinessProbe` | the same kind of check, deciding when the pod gets Service traffic; without one, a container with a port is ready once the port answers |

Every container also gets `POD_NAME`, `POD_NAMESPACE` and `NODE_NAME` in
its environment.

A pod's `volumes` each have a `name` and one source: `"emptyDir": {}` (an
empty folder, deleted with the pod), `"hostPath": {"path": "/some/folder"}`,
`"configMap": {"name": "..."}` or `"secret": {"name": "..."}` (one file per
key).

`restartPolicy` decides what happens when a container exits: `Always` (the
default) restarts it, `OnFailure` restarts it only after a failure, and
`Never` leaves it. Restarts wait 10 seconds, then twice as long each time
the container crashes again, up to 5 minutes. Jobs use `OnFailure` or
`Never` (the default for Jobs); the other controllers always use `Always`.

## How it works

```
                        ┌───────────────────┐
   minikubectl ───────▶ │    API server     │ ◀──── scheduler
                        │  (etcd or a file) │ ◀──── controller manager
                        └───────────────────┘ ◀──── proxy ───▶ Service ports
                             ▲         ▲                           │
                             │         │                           │ TCP
                    kubelet on node-1  kubelet on node-2  ◀────────┘
                      └─ containers       └─ containers
```

The **API server** is the only component that reads and writes the stored
state. Every other component talks to it over HTTP and never to each other.
They learn about changes by **watching**: one long-lived HTTP response that
streams an event for every change to a pod.

Each controller runs a **reconcile loop**: compare how things should be with
how they are, and fix the difference. It reacts to watch events straight
away, and also re-checks everything every few seconds, so a missed event is
never a problem.

How a Deployment becomes running containers:

1. `minikubectl apply` sends the Deployment to the API server.
2. The **Deployment controller** creates a ReplicaSet for the current version of the pod template.
3. The **ReplicaSet controller** creates the pods.
4. The **scheduler** sees the new pods and assigns each one to a node.
5. Each node's **kubelet** sees pods assigned to it, runs their containers, and reports them `Running`, then `Ready` once their port answers.
6. The **proxy** sees ready pods that match a Service's selector and starts sending connections to them.

When the template changes, the Deployment controller creates a second
ReplicaSet and moves pods over one at a time: it adds a new pod, waits until
it is ready, then removes an old one.

### Components

| Program | Package | Role |
|---|---|---|
| `apiserver` | [`pkg/apiserver`](pkg/apiserver), [`pkg/store`](pkg/store) | the HTTP API, and storage in memory backed by etcd or a file |
| `controller-manager` | [`pkg/controller`](pkg/controller), [`pkg/cron`](pkg/cron) | the node, ReplicaSet, Deployment, StatefulSet, DaemonSet, Job and CronJob controllers |
| `scheduler` | [`pkg/scheduler`](pkg/scheduler) | assigns pods to nodes |
| `kubelet` | [`pkg/kubelet`](pkg/kubelet), [`pkg/cri`](pkg/cri) | runs pods on one node: restarts containers, runs probes, sets up volumes and environment, serves logs |
| `proxy` | [`pkg/proxy`](pkg/proxy) | forwards connections on Service ports to ready pods |
| `minikubectl` | [`pkg/client`](pkg/client) | the command-line tool |
| `minik8s` | | starts all of the above in one terminal |

Each program can also be started on its own; run it with `-h` to see its flags.

## Project layout

```
cmd/        one folder per program
pkg/
  api/         the object types: Pod, Deployment, Job, Service, ConfigMap, ...
  apiserver/   HTTP handlers
  store/       objects in memory, saved to etcd or a file
  client/      the Go client for the API, used by every component
  controller/  the reconcile loops
  cron/        reading cron schedules
  scheduler/   scheduling strategies
  kubelet/     running pods on a node
  cri/         running containers: Docker or plain processes
  proxy/       forwarding Service traffic
examples/   object files to apply
learn/      small programs written while learning goroutines, channels and os/exec
```

## Tests

```bash
go test ./...          # every package
go test -race ./...    # with the race detector
```

The kubelet is tested with a fake container runtime, and the controllers and
proxy against a real API server running in memory, so the tests need neither
Docker nor etcd. The etcd storage test only runs when an etcd is available:

```bash
MINIK8S_ETCD=localhost:2379 go test ./pkg/store
```

## Differences from real Kubernetes

The overall design follows Kubernetes, but many things are simplified:

- Objects are JSON files rather than YAML, and are simpler than Kubernetes' own.
- There is one API server. Watches and conflict checks happen in its memory, so several API servers can't share one etcd.
- There is no authentication or authorization: anyone who can reach the API server can change anything.
- Pod networking is the host's: each container's port is published on a free port of the machine, and Service ports are shared by all namespaces.
- `minikubectl logs` asks the kubelet directly instead of going through the API server.
- Events are kept in memory only and are lost when the API server restarts.
- Deleting a namespace or a pod happens at once; there is no `Terminating` state (deleted pods do get a 2-second grace period before their containers are stopped).
- Secrets are stored as plain text, not encrypted.
- Volumes need the Docker runtime, and only `emptyDir`, `hostPath`, ConfigMaps and Secrets exist: there are no persistent volumes, so a StatefulSet's pods don't keep their data when they move.
- Probes check the container's port by HTTP or by connecting; there are no `exec` probes.
- No resource requests or limits, Ingress, network policies or autoscaling.

## License

MIT. See [LICENSE](LICENSE).
