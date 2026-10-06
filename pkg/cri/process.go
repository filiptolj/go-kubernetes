package cri

import (
	"fmt"
	"io"
	"os/exec"
	"sync"
	"syscall"

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

// Start runs the container's command as a process. A process shares this
// machine's network, so it is reachable on its own Port: two copies of the
// same pod on one node would clash.
func (pr *ProcessRuntime) Start(pod api.Pod, c api.Container, logs io.Writer) (Running, error) {
	if len(c.Command) == 0 {
		return Running{}, fmt.Errorf("container %q has no command to run", c.Name)
	}

	cmd := exec.Command(c.Command[0], c.Command[1:]...)
	done, err := start(cmd, logs)
	if err != nil {
		return Running{}, fmt.Errorf("start container %q: %w", c.Name, err)
	}

	pr.mu.Lock()
	pr.procs[processKey(pod, c)] = cmd
	pr.mu.Unlock()

	running := Running{Done: done}
	if c.Port != 0 {
		running.Address = fmt.Sprintf("127.0.0.1:%d", c.Port)
	}
	return running, nil
}

// RemoveAll does nothing: a new kubelet can't find the processes started by
// one that crashed.
func (pr *ProcessRuntime) RemoveAll() error {
	return nil
}

// Stop kills the container's process, and any processes it started.
func (pr *ProcessRuntime) Stop(pod api.Pod, c api.Container) error {
	key := processKey(pod, c)

	pr.mu.Lock()
	cmd, ok := pr.procs[key]
	delete(pr.procs, key)
	pr.mu.Unlock()

	if !ok {
		return nil
	}

	// A negative PID means "the whole process group", so children such as a
	// `sleep` started by `sh -c` are killed too.
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}

// processKey identifies a container's process: "namespace/pod/container".
func processKey(pod api.Pod, c api.Container) string {
	return api.Key(pod.Namespace, pod.Name) + "/" + c.Name
}
