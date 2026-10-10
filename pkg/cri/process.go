package cri

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// ProcessRuntime runs each container's command as a plain process on this
// machine. The image is ignored. It is handy when Docker isn't available.
type ProcessRuntime struct {
	mu    sync.Mutex
	procs map[string]*exec.Cmd
}

// NewProcessRuntime returns a ProcessRuntime, ready to use.
func NewProcessRuntime() *ProcessRuntime {
	return &ProcessRuntime{procs: make(map[string]*exec.Cmd)}
}

// StartSandbox gives the pod nothing to share: plain processes use this
// machine's network, so each container port is reachable at itself.
func (pr *ProcessRuntime) StartSandbox(pod api.Pod) (Sandbox, error) {
	sb := Sandbox{HostPorts: make(map[int]string)}
	for _, c := range slices.Concat(pod.InitContainers, pod.Containers) {
		for _, p := range c.Ports {
			sb.HostPorts[p.ContainerPort] = fmt.Sprintf("127.0.0.1:%d", p.ContainerPort)
		}
	}
	return sb, nil
}

// StopSandbox does nothing: there is no sandbox to remove.
func (pr *ProcessRuntime) StopSandbox(pod api.Pod) error {
	return nil
}

// Start runs the container's command as a process. A process shares this
// machine's network, so it is reachable on its own Port: two copies of the
// same pod on one node would clash.
//
// A plain process can't see volumes at other paths, so pods with volumes
// need the Docker runtime.
func (pr *ProcessRuntime) Start(pod api.Pod, c api.Container, opts Options) (Running, error) {
	if len(c.Command) == 0 {
		return Running{}, fmt.Errorf("container %q has no command to run", c.Name)
	}
	if len(opts.Mounts) > 0 {
		return Running{}, fmt.Errorf("container %q mounts volumes, which only the docker runtime supports", c.Name)
	}

	cmd := exec.Command(c.Command[0], c.Command[1:]...)
	cmd.Env = append(os.Environ(), opts.Env...)
	done, err := start(cmd, opts.Logs)
	if err != nil {
		return Running{}, fmt.Errorf("start container %q: %w", c.Name, err)
	}

	pr.mu.Lock()
	pr.procs[processKey(pod, c)] = cmd
	pr.mu.Unlock()

	running := Running{Done: done, HostPorts: make(map[int]string)}
	for _, p := range c.Ports {
		running.HostPorts[p.ContainerPort] = fmt.Sprintf("127.0.0.1:%d", p.ContainerPort)
	}
	running.Address = running.HostPorts[c.Port()]
	return running, nil
}

// Exec runs the command as another process on this machine: a process
// container has no inside of its own to run it in.
func (pr *ProcessRuntime) Exec(ctx context.Context, pod api.Pod, c api.Container, command []string, stdin io.Reader, out io.Writer) (int, error) {
	if len(command) == 0 {
		return -1, fmt.Errorf("no command to run")
	}
	return runExec(exec.CommandContext(ctx, command[0], command[1:]...), stdin, out)
}

// PodUsage isn't supported: plain processes aren't measured.
func (pr *ProcessRuntime) PodUsage(pods []api.Pod) (map[string]api.Resources, error) {
	return nil, errors.New("the process runtime can't measure what pods use")
}

// RemoveAll does nothing: a new kubelet can't find the processes started by
// one that crashed.
func (pr *ProcessRuntime) RemoveAll() error {
	return nil
}

// Stop kills the container's process, and any processes it started.
func (pr *ProcessRuntime) Stop(pod api.Pod, c api.Container, grace time.Duration) error {
	key := processKey(pod, c)

	pr.mu.Lock()
	cmd, ok := pr.procs[key]
	delete(pr.procs, key)
	pr.mu.Unlock()

	if !ok {
		return nil
	}

	// A negative PID means "the whole process group", so children such as a
	// `sleep` started by `sh -c` get the signal too.
	group := -cmd.Process.Pid
	if grace > 0 && syscall.Kill(group, syscall.SIGTERM) == nil {
		// Signal 0 only checks that the group still exists.
		deadline := time.Now().Add(grace)
		for time.Now().Before(deadline) && syscall.Kill(group, 0) == nil {
			time.Sleep(50 * time.Millisecond)
		}
	}
	err := syscall.Kill(group, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil // it stopped on its own
	}
	return err
}

// processKey identifies a container's process: "namespace/pod/container".
func processKey(pod api.Pod, c api.Container) string {
	return api.Key(pod.Namespace, pod.Name) + "/" + c.Name
}
