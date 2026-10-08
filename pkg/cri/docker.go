package cri

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// DockerRuntime runs each container as a real Docker container, using the
// docker command-line tool.
type DockerRuntime struct {
	// Node is the name of the node this runtime works for. Every container is
	// labelled with it, so a kubelet only ever cleans up its own containers.
	Node string
}

// containerName is the Docker name for a pod's container, such as
// "minik8s-default-web-x7k2p-nginx". Every name starts with "minik8s-", so we
// never touch containers that aren't ours, and includes the namespace, so
// pods with the same name in different namespaces don't clash.
func containerName(pod api.Pod, c api.Container) string {
	return "minik8s-" + pod.Namespace + "-" + pod.Name + "-" + c.Name
}

// Start runs `docker run` for the container. The docker process keeps running
// as long as the container does, and exits with the container's exit code.
// If the container has a Port, it is published on a free port of this machine.
//
// Environment variables are passed in a file only the current user can read,
// not on the command line, where anyone on the machine could see them with
// `ps`: they may come from Secrets.
func (dr DockerRuntime) Start(pod api.Pod, c api.Container, opts Options) (Running, error) {
	if c.Image == "" {
		return Running{}, fmt.Errorf("container %q has no image", c.Name)
	}

	name := containerName(pod, c)

	// Remove a leftover container with the same name, e.g. from a kubelet that
	// crashed. It fails harmlessly if there is none.
	exec.Command("docker", "rm", "-f", name).Run()

	args := []string{"run", "--rm", "--name", name,
		"--label", "minik8s.node=" + dr.Node,
		"--label", "minik8s.namespace=" + pod.Namespace,
		"--label", "minik8s.pod=" + pod.Name}

	var address string
	if c.Port != 0 {
		hostPort, err := freePort()
		if err != nil {
			return Running{}, fmt.Errorf("container %q: find a free port: %w", c.Name, err)
		}
		// Connections to 127.0.0.1:hostPort on this machine reach c.Port inside the container.
		args = append(args, "-p", fmt.Sprintf("127.0.0.1:%d:%d", hostPort, c.Port))
		address = fmt.Sprintf("127.0.0.1:%d", hostPort)
	}

	for _, m := range opts.Mounts {
		volume := m.HostPath + ":" + m.ContainerPath
		if m.ReadOnly {
			volume += ":ro"
		}
		args = append(args, "-v", volume)
	}

	var envFile string
	if len(opts.Env) > 0 {
		f, err := os.CreateTemp("", "minik8s-env-*") // CreateTemp makes it readable by us only
		if err != nil {
			return Running{}, fmt.Errorf("container %q: %w", c.Name, err)
		}
		envFile = f.Name()
		_, err = f.WriteString(strings.Join(opts.Env, "\n") + "\n")
		f.Close()
		if err != nil {
			os.Remove(envFile)
			return Running{}, fmt.Errorf("container %q: %w", c.Name, err)
		}
		args = append(args, "--env-file", envFile)
	}

	args = append(args, c.Image)
	args = append(args, c.Command...)

	done, err := start(exec.Command("docker", args...), opts.Logs)
	if err != nil {
		os.Remove(envFile)
		return Running{}, fmt.Errorf("start container %q: %w", c.Name, err)
	}

	if envFile == "" {
		return Running{Done: done, Address: address}, nil
	}

	// Docker has read the file once the container runs, but we only know that
	// for sure when it exits. Remove the file then, and pass the result on.
	result := make(chan error, 1)
	go func() {
		err := <-done
		os.Remove(envFile)
		result <- err
	}()
	return Running{Done: result, Address: address}, nil
}

// Stop removes the container. Its `docker run` process then exits on its own.
func (dr DockerRuntime) Stop(pod api.Pod, c api.Container) error {
	out, err := exec.Command("docker", "rm", "-f", containerName(pod, c)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("stop container %q: %v: %s", c.Name, err, out)
	}
	return nil
}

// RemoveAll removes every container labelled with this runtime's node.
func (dr DockerRuntime) RemoveAll() error {
	out, err := exec.Command("docker", "ps", "-aq", "--filter", "label=minik8s.node="+dr.Node).Output()
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}

	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return nil
	}

	args := append([]string{"rm", "-f"}, ids...)
	out, err = exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("remove containers: %v: %s", err, out)
	}
	return nil
}
