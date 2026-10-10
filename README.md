# go-kubernetes

A small Kubernetes, written from scratch in Go.

It has the same moving parts as the real thing: an API server backed by etcd,
a scheduler, controllers, a kubelet on every node that runs real Docker
containers, and a proxy that makes Services reachable. Pods, Deployments with
zero-downtime rolling updates, StatefulSets, DaemonSets, Jobs, CronJobs,
Services, ConfigMaps, Secrets, volumes, probes, resource limits, namespaces,
events and a `kubectl`-style CLI all work, and objects are written in the same
YAML as for Kubernetes. Apart from the etcd client and a YAML reader, it uses
only Go's standard library.

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

- **Kubernetes-style objects**: `apiVersion`, `kind`, `metadata`, `spec` and `status`, written in YAML, served under the same URLs (`/api/v1`, `/apis/apps/v1`, ...)
- **Pods** that run as real Docker containers, or as plain processes
- **Init containers** that run one after the other, to completion, before the pod's other containers start
- **Restart policies**: crashed containers are restarted in place, waiting longer each time (`CrashLoopBackOff`)
- **Liveness and readiness probes**, by HTTP request, by running a command in the container, or by checking the port
- **Resource requests and limits**: the scheduler only puts a pod where its CPU and memory requests fit, and Docker enforces the limits; a container that uses too much memory is `OOMKilled`
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
- **Logs** of any container, also followed live with `-f`; **exec** to run a command inside one; **port-forward** to reach a pod's port
- **Label selectors** (`-l app=web`, `env in (prod,dev)`) and full objects as YAML or JSON (`-o yaml`)
- **Watches** of every kind, and **informers**: the controllers, scheduler and proxy keep a local copy of what they need, kept current by a watch, and react to changes at once
- **Optimistic concurrency**: every object has a `resourceVersion`, and an update made from an outdated copy is refused instead of losing someone else's change
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
| `oom.yaml` | a pod that uses more memory than its limit, and is `OOMKilled` |
| `init-and-exec.yaml` | an init container writes a web page before nginx starts; the readiness probe runs a command |

`minikubectl apply -f examples/` applies the whole folder; applying it again
updates what can be updated and leaves the rest as it is.

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
| `get <resource> [name]` | list objects; `-l app=web` picks them by label, `-o yaml`, `-o json` or `-o name` prints them in full, and `get pods -w` keeps watching for changes |
| `describe <resource> <name>` | details and recent events |
| `apply -f <file or folder>` | create the objects in YAML (or JSON) files, several per file separated by `---`; applying again updates them |
| `delete <resource> <name>` | delete an object; a namespace is deleted with everything in it |
| `logs <pod> [-c container] [-f]` | a container's output; `-f` follows it |
| `exec <pod> [-c container] [-i] -- <command>` | run a command in a container; `-i` passes your input to it, and `minikubectl` exits with the command's exit code |
| `port-forward <pod> [local:]<port>` | reach a pod's port at `localhost:local`, until Ctrl+C |
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

Objects are written as in Kubernetes. These are in [`examples/`](examples):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 3
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
      - name: nginx
        image: nginx:1.27
        ports:
        - containerPort: 80
        resources:
          requests:
            cpu: 100m
            memory: 32Mi
          limits:
            memory: 128Mi
---
apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  selector:
    app: web
  ports:
  - port: 8081      # where the Service listens
    targetPort: 80  # the pods' port
```

| Kind | `apiVersion` | Main fields |
|---|---|---|
| `Pod` | `v1` | `spec`: `containers`, `initContainers`, `volumes`, `restartPolicy` |
| `ReplicaSet`, `Deployment`, `StatefulSet` | `apps/v1` | `spec`: `replicas`, `template` (`metadata.labels` and a pod `spec`; Deployments need labels) |
| `DaemonSet` | `apps/v1` | `spec`: `template` |
| `Job` | `batch/v1` | `spec`: `completions` (default 1), `parallelism` (default 1), `backoffLimit` (default 6), `template` |
| `CronJob` | `batch/v1` | `spec`: `schedule` (cron format, such as `*/5 * * * *`), `suspend`, `jobTemplate.spec` (a Job's spec) |
| `Service` | `v1` | `spec`: `selector`, `ports` (`port`, and `targetPort`, which defaults to `port`) |
| `ConfigMap` | `v1` | `data` (keys and values) |
| `Secret` | `v1` | `data` (values in base64) or `stringData` (plain values) |
| `Namespace`, `Node` | `v1` | `metadata.name` |

Every object has a `metadata.name`, and may have `labels`. Objects other
than Nodes and Namespaces can name their `metadata.namespace`; otherwise `-n`
decides, or `default`. The API server fills in `uid`, `creationTimestamp`
and `resourceVersion`.

### Pods and containers

A container has a `name` and an `image`, and optionally:

| Field | Meaning |
|---|---|
| `command` | the command to run, such as `["sh", "-c", "echo hi"]` |
| `ports` | the ports it listens on, `- containerPort: 80`; the kubelet makes them reachable and reports where |
| `env` | environment variables: `{name: MODE, value: fast}`, or a value from a ConfigMap or Secret: `{name: PASSWORD, valueFrom: {secretKeyRef: {name: db, key: password}}}` |
| `volumeMounts` | where the pod's volumes appear: `{name: config, mountPath: /etc/app}` |
| `resources` | `requests` and `limits` of `cpu` (`"0.5"` or `500m`) and `memory` (`64Mi`, `1Gi`); a limit without a request is also the request |
| `livenessProbe` | a check that restarts the container when it fails: `httpGet: {path: /healthz}`, or `exec: {command: [test, -f, /tmp/ok]}`, with `periodSeconds` (default 10) and `failureThreshold` (default 3); with neither, it checks that the port answers |
| `readinessProbe` | the same kind of check, deciding when the pod gets Service traffic; without one, a container with a port is ready once the port answers |

Every container also gets `POD_NAME`, `POD_NAMESPACE` and `NODE_NAME` in
its environment.

`initContainers` are containers too. They run one at a time, each until it
exits, before the others start; meanwhile the pod is `Pending` and its status
says `Init:0/2` and so on. One that fails is run again, or fails the pod if
its `restartPolicy` is `Never`.

A pod's `volumes` each have a `name` and one source: `emptyDir: {}` (an
empty folder, deleted with the pod), `hostPath: {path: /some/folder}`,
`configMap: {name: ...}` or `secret: {secretName: ...}` (one file per key).

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
They learn about changes by **watching**: adding `?watch=true` to a list,
such as `/apis/apps/v1/deployments?watch=true`, turns it into one long-lived
HTTP response that streams an event for every change.

Components don't ask the API server for everything each time they look.
Each keeps **informers**: a local copy of one kind of object, filled by a
list and kept current by a watch. The copy can lag a moment behind, so
controllers remember the changes they have made but not yet seen
(**expectations**) and wait for them before acting again; otherwise a
ReplicaSet controller could count its new pods before they arrive and create
them twice. The scheduler likewise remembers the pods it has just placed.

Each controller runs a **reconcile loop**: compare how things should be with
how they are, and fix the difference. It runs whenever its informers change,
and also every few seconds, for things that depend on time passing.

Every object has a **`resourceVersion`** that changes with every change. An
update that carries an old `resourceVersion` was made from an outdated copy,
and is refused with `409 Conflict`, so two writers can't silently undo each
other's changes.

How a Deployment becomes running containers:

1. `minikubectl apply` sends the Deployment to the API server.
2. The **Deployment controller** creates a ReplicaSet for the current version of the pod template.
3. The **ReplicaSet controller** creates the pods.
4. The **scheduler** sees the new pods and assigns each one to a node. It only considers nodes with enough CPU and memory left for the pod's requests; if none has, the pod stays `Pending` with a `FailedScheduling` event saying why.
5. Each node's **kubelet** sees pods assigned to it, runs their init containers, then their containers, and reports them `Running`, then `Ready` once their port answers.
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
| `kubelet` | [`pkg/kubelet`](pkg/kubelet), [`pkg/cri`](pkg/cri) | runs pods on one node: restarts containers, runs probes, sets up volumes and environment, serves logs and exec |
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

- Objects have Kubernetes' shape but fewer fields, and unknown fields are ignored rather than refused.
- There is one API server. Watches and conflict checks happen in its memory, so several API servers can't share one etcd.
- There is no authentication or authorization: anyone who can reach the API server can change anything.
- Pod networking is the host's: each container's port is published on a free port of the machine, and Service ports are shared by all namespaces.
- `minikubectl logs`, `exec` and `port-forward` talk to the kubelet, or the pod's published port, directly instead of going through the API server; `exec` has no terminal (`-t`).
- A watch only sends changes from the moment it starts; there is no resuming from a `resourceVersion`.
- Events are kept in memory only and are lost when the API server restarts.
- Deleting a namespace or a pod happens at once; there is no `Terminating` state (deleted pods do get a 2-second grace period before their containers are stopped).
- Secrets are stored as plain text, not encrypted.
- Volumes need the Docker runtime, and only `emptyDir`, `hostPath`, ConfigMaps and Secrets exist: there are no persistent volumes, so a StatefulSet's pods don't keep their data when they move.
- Resource limits need the Docker runtime; the process runtime ignores them.
- No Ingress, network policies or autoscaling.

## License

MIT. See [LICENSE](LICENSE).
