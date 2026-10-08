package cri

import (
	"context"
	"errors"
	"fmt"
	"io"
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
// Each of the container's ports is published on a free port of this machine,
// and its resource limits become Docker's limits.
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

	// No --rm: once the container exits, we ask Docker how it ended, and
	// remove it ourselves after that.
	args := []string{"run", "--name", name,
		"--label", "minik8s.node=" + dr.Node,
		"--label", "minik8s.namespace=" + pod.Namespace,
		"--label", "minik8s.pod=" + pod.Name}

	hostPorts := make(map[int]string)
	for _, p := range c.Ports {
		hostPort, err := freePort()
		if err != nil {
			return Running{}, fmt.Errorf("container %q: find a free port: %w", c.Name, err)
		}
		// Connections to 127.0.0.1:hostPort on this machine reach the port inside the container.
		args = append(args, "-p", fmt.Sprintf("127.0.0.1:%d:%d", hostPort, p.ContainerPort))
		hostPorts[p.ContainerPort] = fmt.Sprintf("127.0.0.1:%d", hostPort)
	}
	address := hostPorts[c.Port()]

	args = append(args, limitArgs(c.Resources.Limits)...)

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

	result := make(chan error, 1)
	go func() {
		err := <-done
		// Docker has read the env file once the container runs, but we
		// only know that for sure now.
		if envFile != "" {
			os.Remove(envFile)
		}
		result <- exitReason(name, err)
	}()
	return Running{Done: result, Address: address, HostPorts: hostPorts}, nil
}

// limitArgs turns a container's limits into `docker run` options. --cpus
// limits CPU time: "0.5" lets it use half of one CPU. --memory makes the
// kernel kill the container if it uses more; --memory-swap set to the same
// amount stops it from swapping instead, so the limit really holds.
func limitArgs(limits api.ResourceList) []string {
	var args []string
	r := api.ParseResources(limits)
	if _, ok := limits[api.ResourceCPU]; ok && r.CPU > 0 {
		args = append(args, fmt.Sprintf("--cpus=%.3f", float64(r.CPU)/1000))
	}
	if _, ok := limits[api.ResourceMemory]; ok && r.Memory > 0 {
		args = append(args, fmt.Sprintf("--memory=%db", r.Memory), fmt.Sprintf("--memory-swap=%db", r.Memory))
	}
	return args
}

// exitReason asks Docker how a container that just exited ended, then
// removes it. It returns err, with the reason filled in if the kernel killed
// the container for using more memory than its limit.
func exitReason(name string, err error) error {
	out, inspectErr := exec.Command("docker", "inspect", "--format", "{{.State.OOMKilled}}", name).Output()
	exec.Command("docker", "rm", "-f", name).Run()

	var exit *ExitError
	if errors.As(err, &exit) && inspectErr == nil && strings.TrimSpace(string(out)) == "true" {
		exit.Reason = "OOMKilled"
	}
	return err
}

// Exec runs `docker exec` in the container. With stdin, it passes -i so the
// command can read it.
func (dr DockerRuntime) Exec(ctx context.Context, pod api.Pod, c api.Container, command []string, stdin io.Reader, out io.Writer) (int, error) {
	args := []string{"exec"}
	if stdin != nil {
		args = append(args, "-i")
	}
	args = append(args, containerName(pod, c))
	args = append(args, command...)
	return runExec(exec.CommandContext(ctx, "docker", args...), stdin, out)
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
