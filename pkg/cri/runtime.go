// Package cri runs the containers of a pod. It is a tiny version of the
// Container Runtime Interface that real Kubernetes uses to talk to Docker,
// containerd and others.
package cri

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"syscall"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// Runtime starts and stops containers.
type Runtime interface {
	// StartSandbox prepares what a pod's containers share, before any of
	// them start: its network, with its IP address and published ports.
	StartSandbox(pod api.Pod) (Sandbox, error)

	// StopSandbox removes a pod's sandbox, once its containers are gone.
	StopSandbox(pod api.Pod) error

	// Start starts one container of a pod and returns at once.
	Start(pod api.Pod, c api.Container, opts Options) (Running, error)

	// Stop stops a container that Start started: it asks the container to
	// stop with SIGTERM, gives it grace to exit, and then kills it.
	Stop(pod api.Pod, c api.Container, grace time.Duration) error

	// Exec runs a command inside a running container, with stdin (if not
	// nil) as its input and its output written to out, and returns its
	// exit code. The error is for when the command couldn't be run at all.
	Exec(ctx context.Context, pod api.Pod, c api.Container, command []string, stdin io.Reader, out io.Writer) (int, error)

	// RemoveAll removes every container left over from an earlier run of
	// this kubelet, for example one that crashed.
	RemoveAll() error

	// PodUsage measures how much CPU and memory each pod's containers use
	// right now, by the pod's "namespace/name".
	PodUsage(pods []api.Pod) (map[string]api.Resources, error)
}

// Options are what a container starts with, besides its image and command.
// The kubelet works them out from the pod.
type Options struct {
	Sandbox Sandbox   // the pod's sandbox, from StartSandbox
	Logs    io.Writer // where the container's output goes
	Env     []string  // environment variables, as "NAME=value"
	Mounts  []Mount   // folders of this machine to show inside the container
}

// Mount puts a folder of this machine at a path inside a container.
type Mount struct {
	HostPath      string
	ContainerPath string
	ReadOnly      bool
}

// Sandbox is what a pod's containers share: their network.
type Sandbox struct {
	// IP is the pod's address on the cluster network, or "" if the
	// runtime doesn't give pods one.
	IP string

	// HostPorts says where each port of the pod's containers can be
	// reached from this machine, as host:port, by container port.
	HostPorts map[int]string
}

// Running is a container that Start started.
type Running struct {
	// Done receives the container's result when it exits: nil for exit code
	// 0, otherwise an error, usually an *ExitError.
	Done <-chan error

	// HostPorts says where each of the container's ports can be reached from
	// this machine, as host:port, by container port. Address is the same
	// for its first port; it is empty if the container has no ports.
	HostPorts map[int]string
	Address   string
}

// ExitError is how a container ended when it didn't succeed.
type ExitError struct {
	Code   int    // its exit code
	Reason string // why, if known, such as "OOMKilled"
}

func (e *ExitError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("%s (exit code %d)", e.Reason, e.Code)
	}
	return fmt.Sprintf("exit code %d", e.Code)
}

// exitError turns the error of a finished process into an *ExitError, if it
// says how the process exited.
func exitError(err error) error {
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return &ExitError{Code: exit.ExitCode()}
	}
	return err
}

// runExec runs cmd to completion and returns its exit code. An error means
// it couldn't be run, or was stopped before it finished.
func runExec(cmd *exec.Cmd, stdin io.Reader, out io.Writer) (int, error) {
	cmd.Stdin = stdin
	cmd.Stdout = out
	cmd.Stderr = out
	err := cmd.Run()

	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() >= 0 {
		return exit.ExitCode(), nil
	}
	if err != nil {
		return -1, err
	}
	return 0, nil
}

// start runs cmd in the background and returns a channel that receives the
// result of cmd.Wait when the process exits.
func start(cmd *exec.Cmd, logs io.Writer) (<-chan error, error) {
	cmd.Stdout = logs
	cmd.Stderr = logs

	// Put the process in its own process group. Then Ctrl+C in the kubelet's
	// terminal doesn't also hit the containers: the kubelet stops them itself.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	done := make(chan error, 1)
	go func() {
		done <- exitError(cmd.Wait())
	}()
	return done, nil
}
