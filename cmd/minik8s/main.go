// Command minik8s starts a whole cluster with one command: the API server,
// the controller manager, the scheduler, the proxy and a kubelet per node.
// With the Docker runtime, it also starts the cluster's DNS and Service
// proxy inside the cluster network, as a container.
// Each runs as its own process, like in real Kubernetes; minik8s starts them
// in the right order, prints their logs with the component's name in front,
// and stops them all on Ctrl+C.
//
// Build everything, then run it:
//
//	go build -o bin/ ./cmd/...
//	./bin/minik8s
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/cri"
)

func main() {
	nodes := flag.Int("nodes", 2, "how many nodes (kubelets) to run")
	port := flag.Int("port", 8080, "port for the API server")
	kubeletPort := flag.Int("kubelet-port", 10250, "port for the first kubelet's logs; the next kubelets use the ports after it")
	runtime := flag.String("runtime", "docker", "how kubelets run containers: docker or process")
	dataFile := flag.String("data", "data/apiserver.json", "file the API server saves the cluster in (empty: memory only)")
	etcd := flag.String("etcd", "", "save the cluster in etcd at this address, such as localhost:2379, instead of a file")
	binDir := flag.String("bin", "", "folder with the component programs (default: the folder minik8s is in)")
	ingressPort := flag.Int("ingress-port", 8090, "port on 127.0.0.1 where Ingresses are served (0: none)")
	flag.Parse()

	dir, err := findBinaries(*binDir)
	if err != nil {
		log.Fatal(err)
	}

	server := fmt.Sprintf("http://localhost:%d", *port)

	// The API server goes first: everything else talks to it.
	// 0.0.0.0, not just ":port": the cluster proxy's container reaches the API
	// server through Docker's host.docker.internal, and on WSL that only
	// arrives at IPv4 listeners. ":port" would listen on IPv6 ([::]) too.
	apiserver := component{name: "apiserver", args: []string{"-addr", fmt.Sprintf("0.0.0.0:%d", *port), "-data", *dataFile}}
	if *etcd != "" {
		apiserver.args = append(apiserver.args, "-etcd", *etcd)
	}

	others := []component{
		{name: "controller-manager", args: []string{"-server", server}},
		{name: "scheduler", args: []string{"-server", server}},
		{name: "proxy", args: []string{"-server", server}},
	}
	if *ingressPort != 0 {
		others[2].args = append(others[2].args, "-ingress-addr", fmt.Sprintf("127.0.0.1:%d", *ingressPort))
	}
	if *runtime == "docker" {
		err := cri.EnsureNetwork()
		if err != nil {
			log.Fatal(err)
		}
		others = append(others, clusterProxy(dir, *port))
	}
	for i := range *nodes {
		node := fmt.Sprintf("node-%d", i+1)
		others = append(others, component{
			name:  "kubelet",
			label: "kubelet " + node,
			args: []string{"-server", server, "-node", node, "-runtime", *runtime,
				"-port", fmt.Sprint(*kubeletPort + i)},
		})
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	c := &cluster{dir: dir, out: newOutput()}
	err = c.run(ctx, apiserver, others, server)
	if err != nil {
		c.out.say("minik8s", "%v", err)
		os.Exit(1)
	}
}

// findBinaries returns the folder holding the component programs, and checks
// they are all there.
func findBinaries(dir string) (string, error) {
	if dir == "" {
		self, err := os.Executable()
		if err != nil {
			return "", err
		}
		dir = filepath.Dir(self)
	}
	// Docker needs a full path to mount the proxy program.
	dir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}

	for _, name := range []string{"apiserver", "controller-manager", "scheduler", "proxy", "kubelet"} {
		_, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			return "", fmt.Errorf("can't find %s in %s: build everything first with `go build -o bin/ ./cmd/...`, then run ./bin/minik8s", name, dir)
		}
	}
	return dir, nil
}

// clusterProxyName is the Docker name of the cluster-proxy container.
const clusterProxyName = "minik8s-cluster-proxy"

// clusterProxy is the proxy program again, run in a container on the
// cluster network at a fixed address: there it serves the cluster's DNS
// and forwards Service ports to pod IPs, so pods can reach Services by
// name. Its image only needs to provide the C library the program uses; the
// program itself comes from this machine. It reaches the API server on this
// machine through host.docker.internal.
func clusterProxy(dir string, apiPort int) component {
	exec.Command("docker", "rm", "-f", clusterProxyName).Run() // a leftover from a crash
	return component{
		program: "docker",
		label:   "cluster-proxy",
		args: []string{"run", "--rm", "--name", clusterProxyName,
			"--label", "minik8s=true",
			"--network", cri.NetworkName, "--ip", cri.ClusterProxyIP,
			"--add-host", "host.docker.internal:host-gateway",
			"-v", filepath.Join(dir, "proxy") + ":/proxy:ro",
			"busybox:1.36-glibc", "/proxy",
			"-server", fmt.Sprintf("http://host.docker.internal:%d", apiPort),
			"-bind", "0.0.0.0", "-pod-ips",
			"-dns", ":53", "-dns-service-ip", cri.ClusterProxyIP, "-domain", cri.ClusterDomain},
	}
}

// component is one program of the cluster.
type component struct {
	name    string   // the program's file name, such as "kubelet"
	program string   // a program to run instead, found on the PATH, such as "docker"
	label   string   // what to print in front of its log lines; name if empty
	args    []string // its command-line flags
}

// process is a running component.
type process struct {
	label string
	cmd   *exec.Cmd
	done  chan error // receives the result of cmd.Wait when it exits
}

// cluster starts and stops the components.
type cluster struct {
	dir string
	out *output

	mu       sync.Mutex
	running  []*process
	stopping bool
}

// run starts the API server, waits until it answers, starts the other
// components, and then waits. When ctx is cancelled (Ctrl+C), or a component
// exits by itself, it stops them all.
func (c *cluster) run(ctx context.Context, apiserver component, others []component, server string) error {
	exited := make(chan *process, 1)

	api, err := c.start(apiserver, exited)
	if err != nil {
		return err
	}

	err = waitForAPIServer(ctx, server, api)
	if err != nil {
		c.stopAll()
		return err
	}
	c.out.say("minik8s", "API server is up at %s", server)

	for _, comp := range others {
		_, err := c.start(comp, exited)
		if err != nil {
			c.stopAll()
			return err
		}
	}
	c.out.say("minik8s", "cluster started; press Ctrl+C to stop it")

	var failure error
	select {
	case <-ctx.Done():
		c.out.say("minik8s", "stopping the cluster...")
	case p := <-exited:
		failure = fmt.Errorf("%s exited unexpectedly, so the whole cluster is stopping", p.label)
		c.out.say("minik8s", "%v", failure)
	}

	c.stopAll()
	c.out.say("minik8s", "cluster stopped")
	return failure
}

// start starts one component. If it exits on its own, it is sent to exited.
func (c *cluster) start(comp component, exited chan<- *process) (*process, error) {
	label := comp.label
	if label == "" {
		label = comp.name
	}

	path := filepath.Join(c.dir, comp.name)
	if comp.program != "" {
		path = comp.program
	}
	cmd := exec.Command(path, comp.args...)

	// The component's output, both normal and error, goes through a pipe:
	// everything it writes comes out of the other end, line by line, and is
	// printed with its label in front.
	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw
	go c.out.copyLines(label, pr)

	// Its own process group, so Ctrl+C in this terminal doesn't reach it
	// directly: minik8s stops the components itself, in the right order.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	err := cmd.Start()
	if err != nil {
		pw.Close()
		return nil, fmt.Errorf("start %s: %w", label, err)
	}

	p := &process{label: label, cmd: cmd, done: make(chan error, 1)}
	c.mu.Lock()
	c.running = append(c.running, p)
	c.mu.Unlock()

	go func() {
		err := cmd.Wait()
		pw.Close() // no more output: lets copyLines finish
		p.done <- err

		c.mu.Lock()
		stopping := c.stopping
		c.mu.Unlock()
		if !stopping {
			select {
			case exited <- p:
			default: // another component already reported; one is enough
			}
		}
	}()
	return p, nil
}

// stopAll stops the components in the reverse order they started: the
// kubelets first, so they can still tell the API server about their pods
// and remove their containers, and the API server last.
func (c *cluster) stopAll() {
	c.mu.Lock()
	c.stopping = true
	running := c.running
	c.mu.Unlock()

	for i := len(running) - 1; i >= 0; i-- {
		stop(running[i])
	}
	// In case the cluster proxy's container outlived its `docker run`.
	exec.Command("docker", "rm", "-f", clusterProxyName).Run()
}

// stop asks a process to stop, the way Ctrl+C would, and waits for it. If it
// hasn't stopped after 15 seconds, it is killed.
func stop(p *process) {
	select {
	case <-p.done:
		return // it already stopped
	default:
	}

	p.cmd.Process.Signal(os.Interrupt)
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		p.cmd.Process.Kill()
		<-p.done
	}
}

// waitForAPIServer waits until the API server answers on /healthz. It gives
// up if the API server exits (for example because its port is taken), or
// after 15 seconds.
func waitForAPIServer(ctx context.Context, server string, api *process) error {
	deadline := time.After(15 * time.Second)
	for {
		resp, err := http.Get(server + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		select {
		case err := <-api.done:
			api.done <- err // put it back, so stop() still sees it
			return errors.New("the API server exited while starting; see its messages above")
		case <-deadline:
			return errors.New("the API server didn't answer within 15 seconds")
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// output prints lines from all components, one whole line at a time, each
// with the component's label in front.
type output struct {
	mu     sync.Mutex
	colors bool
	next   int               // next color to hand out
	color  map[string]string // by label
}

func newOutput() *output {
	// Colors only make sense on a terminal, not when output goes to a file.
	info, err := os.Stdout.Stat()
	colors := err == nil && info.Mode()&os.ModeCharDevice != 0 && os.Getenv("NO_COLOR") == ""
	return &output{colors: colors, color: make(map[string]string)}
}

// ANSI escape codes for a few colors that are readable on dark and light terminals.
var palette = []string{"\033[36m", "\033[33m", "\033[35m", "\033[32m", "\033[34m", "\033[91m", "\033[96m"}

const reset = "\033[0m"

// copyLines prints every line read from r, with label in front.
func (o *output) copyLines(label string, r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		o.print(label, scanner.Text())
	}
}

// say prints a message of minik8s's own.
func (o *output) say(label, format string, args ...any) {
	o.print(label, fmt.Sprintf(format, args...))
}

// print writes one line with its label. The lock keeps lines from different
// components from getting mixed up halfway.
func (o *output) print(label, line string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	prefix := fmt.Sprintf("%-19s|", label)
	if o.colors {
		c, ok := o.color[label]
		if !ok {
			c = palette[o.next%len(palette)]
			o.next++
			o.color[label] = c
		}
		prefix = c + prefix + reset
	}
	fmt.Println(prefix, strings.TrimRight(line, "\r"))
}
