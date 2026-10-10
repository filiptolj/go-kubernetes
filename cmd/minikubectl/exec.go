package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
)

// exec runs a command in a container, through the kubelet of its node:
//
//	exec <pod> [-c container] [-i] -- <command> [args...]
//
// The command's output is printed as it comes, and minikubectl exits with
// the command's exit code. With -i, minikubectl's own input is passed to the
// command, as in `echo hi | minikubectl exec web -i -- cat`.
func (c *cli) exec(args []string) error {
	const usage = "usage: minikubectl exec <pod> [-c container] [-i] -- <command> [args...]"
	dash := -1
	for i, arg := range args {
		if arg == "--" {
			dash = i
			break
		}
	}
	if len(args) == 0 || dash < 1 || dash == len(args)-1 {
		return errors.New(usage)
	}
	podName, command := args[0], args[dash+1:]

	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	container := fs.String("c", "", "container to run the command in (needed if the pod has more than one)")
	stdin := fs.Bool("i", false, "pass this program's input to the command")
	if err := fs.Parse(args[1:dash]); err != nil {
		return err
	}

	pod, name, kubelet, err := c.findContainer(podName, *container)
	if err != nil {
		return err
	}

	query := url.Values{"command": command}
	var body io.Reader
	if *stdin {
		query.Set("stdin", "true")
		body = os.Stdin
	}
	execURL := kubelet + "/exec/" + url.PathEscape(pod.Namespace) + "/" + url.PathEscape(podName) + "/" +
		url.PathEscape(name) + "?" + query.Encode()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, execURL, body)
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
		return fmt.Errorf("kubelet: %s", strings.TrimSpace(string(msg)))
	}

	_, err = io.Copy(os.Stdout, resp.Body)
	if err != nil {
		return err
	}

	// The exit code comes after the output, in a trailer, which can only be
	// read once the body has been read to the end.
	code, err := strconv.Atoi(resp.Trailer.Get("X-Exit-Code"))
	if err != nil {
		return errors.New("the kubelet didn't say how the command ended")
	}
	if code != 0 {
		return exitCode(code)
	}
	return nil
}

// portForward makes a port of a pod reachable on this machine:
//
//	port-forward <pod> [local-port:]<pod-port>
//
// It listens on 127.0.0.1:local-port, and copies every connection to and
// from the place the pod's port is published, until Ctrl+C.
func (c *cli) portForward(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: minikubectl port-forward <pod> [local-port:]<pod-port>")
	}
	podName := args[0]
	localText, podText, found := strings.Cut(args[1], ":")
	if !found {
		podText = localText
	}
	local, err1 := strconv.Atoi(localText)
	podPort, err2 := strconv.Atoi(podText)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("ports must be numbers, like 8080:80, not %q", args[1])
	}

	pod, err := c.client.GetPod(c.namespace, podName)
	if err != nil {
		return err
	}
	target := pod.HostPorts[podPort]
	if target == "" {
		return fmt.Errorf("pod %q doesn't publish port %d (is it running, and does a container list that port?)", podName, podPort)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", local))
	if err != nil {
		return err
	}
	fmt.Printf("Forwarding from %s -> %d\n", l.Addr(), podPort)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	go func() {
		<-ctx.Done()
		l.Close() // makes Accept below return
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			return nil // closed by Ctrl+C
		}
		go forward(conn, target)
	}
}

// forward copies data both ways between conn and a new connection to target,
// until either side closes.
func forward(conn net.Conn, target string) {
	defer conn.Close()
	backend, err := net.Dial("tcp", target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to the pod: %v\n", err)
		return
	}
	defer backend.Close()

	var wg sync.WaitGroup
	wg.Go(func() {
		io.Copy(backend, conn)
		backend.(*net.TCPConn).CloseWrite() // tell the pod we're done sending
	})
	io.Copy(conn, backend)
	conn.Close() // the pod is done: end the other copy too
	wg.Wait()
}
