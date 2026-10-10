// Package kubelet is the agent that runs on every node. It runs the pods the
// scheduler bound to its node, keeps their containers running as their
// restart policy says, and reports how they are doing.
package kubelet

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/cri"
)

// Config describes the node a kubelet manages.
type Config struct {
	NodeName  string
	CPU       int
	Memory    int
	LogDir    string        // container logs go to LogDir/<namespace>/<pod>/<container>.log
	VolumeDir string        // a pod's volumes go to VolumeDir/<namespace>/<pod>/<volume>
	Heartbeat time.Duration // how often to tell the API server the node is alive

	// ListenAddr is where to serve container logs over HTTP, such as ":10250".
	// Empty means don't serve them.
	ListenAddr string

	// GracePeriod is how long a deleted pod keeps running before it is
	// stopped. It gives the proxy time to stop sending it new connections,
	// and lets requests already in progress finish.
	GracePeriod time.Duration

	// A container that exits is restarted after RestartDelay. While it keeps
	// crashing, the delay doubles each time, up to MaxRestartDelay. Once a
	// container has run for HealthyAfter, the delay starts over.
	RestartDelay    time.Duration
	MaxRestartDelay time.Duration
	HealthyAfter    time.Duration

	// Resync is how often to look at every pod again, in case a watch event
	// was missed or a pod is waiting for an older one with the same name.
	Resync time.Duration
}

// Kubelet runs the pods bound to one node.
type Kubelet struct {
	cfg     Config
	client  *client.Client
	runtime cri.Runtime
	events  *client.Recorder
	address string // where this kubelet serves logs, e.g. "http://localhost:10250"

	mu   sync.Mutex
	pods map[string]*runningPod // pods this kubelet runs, by "namespace/name"
}

// New returns a Kubelet that talks to the API server through c and runs
// containers with rt.
//
// Durations left at zero in cfg get sensible defaults.
func New(cfg Config, c *client.Client, rt cri.Runtime) *Kubelet {
	setDefault(&cfg.Heartbeat, 5*time.Second)
	setDefault(&cfg.RestartDelay, 10*time.Second)
	setDefault(&cfg.MaxRestartDelay, 5*time.Minute)
	setDefault(&cfg.HealthyAfter, time.Minute)
	setDefault(&cfg.Resync, 10*time.Second)
	if cfg.VolumeDir == "" {
		cfg.VolumeDir = filepath.Join(os.TempDir(), "minik8s", "volumes", cfg.NodeName)
	}

	return &Kubelet{
		cfg:     cfg,
		client:  c,
		runtime: rt,
		events:  c.Recorder("kubelet on " + cfg.NodeName),
		pods:    make(map[string]*runningPod),
	}
}

// setDefault sets *d to def if it is zero.
func setDefault(d *time.Duration, def time.Duration) {
	if *d == 0 {
		*d = def
	}
}

// Run registers the node, then runs the pods bound to it until ctx is
// cancelled. If the API server goes away, the pods keep running and the
// kubelet reconnects. On the way out it stops its pods and marks the node
// NotReady.
func (k *Kubelet) Run(ctx context.Context) error {
	// Containers left over from an earlier run have nobody watching them.
	// Start from a clean slate.
	err := k.runtime.RemoveAll()
	if err != nil {
		log.Printf("could not remove old containers: %v", err)
	}

	if k.cfg.ListenAddr != "" {
		l, err := net.Listen("tcp", k.cfg.ListenAddr)
		if err != nil {
			return fmt.Errorf("serve logs: %w", err)
		}
		k.address = fmt.Sprintf("http://localhost:%d", l.Addr().(*net.TCPAddr).Port)

		srv := &http.Server{Handler: k.handler()}
		go srv.Serve(l)
		defer srv.Close()
		log.Printf("serving container logs on %s", k.address)
	}

	err = client.Retry(ctx, "register node", func() error {
		return k.heartbeat(true)
	})
	if err != nil {
		return nil // cancelled before the API server was ever reachable
	}
	log.Printf("registered node %q", k.cfg.NodeName)
	k.events.Normal("Node", "", k.cfg.NodeName, "KubeletStarted", "kubelet started")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		k.heartbeatLoop(ctx)
	}()

	client.Retry(ctx, "watch pods", func() error {
		return k.sync(ctx)
	})

	cancel()
	<-heartbeatDone
	k.shutdown()
	return nil
}

// sync lists the pods, then watches them, starting and stopping pods on this
// node as needed. Every Resync it lists them all again. It returns when the
// watch ends.
func (k *Kubelet) sync(ctx context.Context) error {
	// Start watching before listing, so no pod can slip through the gap between them.
	events, err := k.client.WatchPods(ctx)
	if err != nil {
		return err
	}

	err = k.syncAll()
	if err != nil {
		return err
	}

	resync := time.NewTicker(k.cfg.Resync)
	defer resync.Stop()

	for {
		select {
		case event, ok := <-events:
			if !ok {
				return errors.New("lost connection to the API server")
			}
			if event.Type == api.EventDeleted {
				k.stopPodLater(event.Object)
				continue
			}
			k.handle(event.Object)
		case <-resync.C:
			err := k.syncAll()
			if err != nil {
				log.Printf("resync: %v", err)
			}
		}
	}
}

// syncAll looks at every pod.
func (k *Kubelet) syncAll() error {
	pods, err := k.client.ListPods("") // every namespace
	if err != nil {
		return err
	}
	for _, pod := range pods {
		k.recover(pod)
		k.handle(pod)
	}
	return nil
}

// recover fails pods that are marked Running on this node but that this
// kubelet isn't running: they were started by an earlier kubelet that has
// since died, and their containers were removed when this one started. If
// such a pod belongs to a controller, the controller replaces it.
func (k *Kubelet) recover(pod api.Pod) {
	if pod.NodeName != k.cfg.NodeName || pod.Phase != api.PodRunning {
		return
	}

	key := api.Key(pod.Namespace, pod.Name)
	k.mu.Lock()
	_, ours := k.pods[key]
	k.mu.Unlock()
	if ours {
		return
	}

	// pod may come from a list made a moment ago, just before a pod of ours
	// finished and was forgotten. Only a fresh copy can tell.
	fresh, err := k.client.GetPod(pod.Namespace, pod.Name)
	if err != nil || fresh.UID != pod.UID || fresh.Phase != api.PodRunning {
		return
	}

	log.Printf("pod %s was left running by an earlier kubelet: failing it", key)
	k.events.Warning("Pod", pod.Namespace, pod.Name, "Lost", "the kubelet on %s restarted while this pod ran; its containers are gone", k.cfg.NodeName)
	err = k.client.SetPodPhase(pod.Namespace, pod.Name, api.PodFailed)
	if err != nil {
		log.Printf("could not report pod %s as Failed: %v", key, err)
	}
}

// handle starts a pod if it is bound to this node and still waiting to run.
func (k *Kubelet) handle(pod api.Pod) {
	if pod.NodeName != k.cfg.NodeName || pod.Phase != api.PodPending {
		return
	}

	key := api.Key(pod.Namespace, pod.Name)
	k.mu.Lock()
	old, exists := k.pods[key]
	if exists && old.pod.UID == pod.UID {
		k.mu.Unlock()
		return // already running it
	}
	if exists {
		// An older pod with the same name is still here, in its grace period
		// or because we missed its deletion. Make sure it's on its way out;
		// the new one starts on a later resync, once the old one is gone.
		k.mu.Unlock()
		k.stopPod(old.pod)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	rp := newRunningPod(pod, cancel)
	k.pods[key] = rp
	k.mu.Unlock()

	k.startPod(ctx, rp)
}

// stopPodLater stops a deleted pod once the grace period is over, and
// removes its logs. A pod that had already finished only has logs to remove.
func (k *Kubelet) stopPodLater(pod api.Pod) {
	stop := func() {
		k.stopPod(pod)
		k.removeLogs(pod)
	}

	if k.cfg.GracePeriod == 0 {
		stop()
		return
	}

	// time.AfterFunc runs the function in its own goroutine once the time is
	// up. Meanwhile the kubelet carries on handling other pods.
	time.AfterFunc(k.cfg.GracePeriod, stop)
}

// stopPod stops a pod this kubelet runs, and forgets it. Only the exact
// pod, with the same UID, is stopped: a newer pod with the same name is left
// alone. It returns false if the pod isn't running here.
func (k *Kubelet) stopPod(pod api.Pod) bool {
	key := api.Key(pod.Namespace, pod.Name)

	k.mu.Lock()
	rp, ok := k.pods[key]
	if !ok || rp.pod.UID != pod.UID || rp.stopping {
		k.mu.Unlock()
		return false
	}
	rp.stopping = true
	k.mu.Unlock()

	log.Printf("stopping pod %s", key)
	k.events.Normal("Pod", pod.Namespace, pod.Name, "Killing", "stopping the pod's containers")

	rp.cancel() // the container loops stop restarting
	for _, c := range slices.Concat(rp.pod.InitContainers, rp.pod.Containers) {
		err := k.runtime.Stop(rp.pod, c)
		if err != nil {
			log.Printf("pod %s: %v", key, err)
		}
	}
	rp.wg.Wait() // until every container loop has finished
	k.removeSandbox(rp)
	k.removeVolumes(rp.pod)

	k.mu.Lock()
	delete(k.pods, key)
	k.mu.Unlock()
	return true
}

// shutdown stops every pod this kubelet runs, reports them Failed because
// their node is going away, and marks the node NotReady.
func (k *Kubelet) shutdown() {
	k.mu.Lock()
	var running []api.Pod
	for _, rp := range k.pods {
		running = append(running, rp.pod)
	}
	k.mu.Unlock()

	// All at once: one after the other, a node with many pods can take
	// longer than whoever is stopping the kubelet is willing to wait.
	var wg sync.WaitGroup
	for _, pod := range running {
		wg.Go(func() {
			if k.stopPod(pod) {
				err := k.client.SetPodPhase(pod.Namespace, pod.Name, api.PodFailed)
				if err != nil {
					log.Printf("could not report pod %s as Failed: %v", api.Key(pod.Namespace, pod.Name), err)
				}
			}
		})
	}
	wg.Wait()

	err := k.heartbeat(false)
	if err != nil {
		log.Printf("could not mark node %q NotReady: %v", k.cfg.NodeName, err)
		return
	}
	log.Printf("marked node %q NotReady", k.cfg.NodeName)
	k.events.Normal("Node", "", k.cfg.NodeName, "KubeletStopped", "kubelet shut down, node marked NotReady")
}

// isRunning reports whether this kubelet runs a pod, by its "namespace/name".
func (k *Kubelet) isRunning(key string) bool {
	k.mu.Lock()
	defer k.mu.Unlock()

	_, ok := k.pods[key]
	return ok
}

// heartbeat registers the node with the API server, or refreshes it.
func (k *Kubelet) heartbeat(ready bool) error {
	return k.client.PutNode(api.Node{
		TypeMeta:   api.TypeMetaFor("Node"),
		ObjectMeta: api.ObjectMeta{Name: k.cfg.NodeName},
		NodeStatus: api.NodeStatus{
			Capacity: api.ResourceList{
				"cpu":    fmt.Sprint(k.cfg.CPU),
				"memory": fmt.Sprintf("%dMi", k.cfg.Memory),
			},
			Ready:   ready,
			Address: k.address,
		},
	})
}

// heartbeatLoop sends a heartbeat every cfg.Heartbeat until ctx is cancelled.
func (k *Kubelet) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(k.cfg.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := k.heartbeat(true)
			if err != nil {
				log.Printf("heartbeat failed: %v", err)
			}
		}
	}
}
