// Package cri runs the containers of a pod. It is a tiny version of the
// Container Runtime Interface that real Kubernetes uses to talk to Docker,
// containerd and others.
package cri

import (
	"io"
	"net"
	"os/exec"
	"syscall"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// Runtime starts and stops containers.
type Runtime interface {
	// Start starts one container of a pod and returns at once. Its output goes
	// to logs.
	Start(pod api.Pod, c api.Container, logs io.Writer) (Running, error)

	// Stop kills a container that Start started.
	Stop(pod api.Pod, c api.Container) error

	// RemoveAll removes every container left over from an earlier run of
	// this kubelet, for example one that crashed.
	RemoveAll() error
}

// Running is a container that Start started.
type Running struct {
	// Done receives the container's result when it exits: nil for exit code
	// 0, an error otherwise.
	Done <-chan error

	// Address is host:port where the container's Port can be reached from
	// this machine. Empty if the container has no Port.
	Address string
}

// freePort asks the operating system for a TCP port that nobody is using.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()

	// Addr returns the general net.Addr interface. For a TCP listener the
	// value inside is a *net.TCPAddr, which has the Port field we need.
	return l.Addr().(*net.TCPAddr).Port, nil
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
		done <- cmd.Wait()
	}()
	return done, nil
}
