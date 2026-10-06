package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strings"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// logs handles `logs <pod> [-c container] [-f]`. It prints a container's
// output, fetched from the kubelet of the node that runs the pod.
func (c *cli) logs(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: minikubectl logs <pod> [-c container] [-f]")
	}
	podName := args[0]

	// The logs command has flags of its own, so it gets its own FlagSet.
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	container := fs.String("c", "", "container to show (needed if the pod has more than one)")
	follow := fs.Bool("f", false, "keep printing new output until Ctrl+C")
	err := fs.Parse(args[1:])
	if err != nil {
		return err
	}

	pod, err := c.client.GetPod(c.namespace, podName)
	if err != nil {
		return err
	}

	if *container == "" {
		if len(pod.Containers) != 1 {
			return fmt.Errorf("pod %q has %d containers: choose one with -c", podName, len(pod.Containers))
		}
		*container = pod.Containers[0].Name
	}

	if pod.NodeName == "" {
		return fmt.Errorf("pod %q isn't scheduled on a node yet, so it has no logs", podName)
	}

	// Logs live on the node that ran the pod, so ask that node's kubelet.
	nodes, err := c.client.ListNodes()
	if err != nil {
		return err
	}
	i := slices.IndexFunc(nodes, func(n api.Node) bool { return n.Name == pod.NodeName })
	if i < 0 || nodes[i].Address == "" {
		return fmt.Errorf("node %q has no kubelet address to ask for logs", pod.NodeName)
	}

	logsURL := nodes[i].Address + "/logs/" + url.PathEscape(pod.Namespace) + "/" +
		url.PathEscape(podName) + "/" + url.PathEscape(*container)
	if *follow {
		logsURL += "?follow=true"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, logsURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("ask kubelet of node %q: %w", pod.NodeName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("kubelet of node %q: %s", pod.NodeName, strings.TrimSpace(string(msg)))
	}

	_, err = io.Copy(os.Stdout, resp.Body)
	if err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}
