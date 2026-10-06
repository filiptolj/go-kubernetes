package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// Client talks to the mini-apiserver over HTTP.
type Client struct {
	baseURL string
	http    *http.Client
}

// New returns a Client for the API server at baseURL, e.g. "http://localhost:8080".
func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 5 * time.Second},
	}
}

// listPath returns the URL path for listing one kind of namespaced object,
// such as "pods": in one namespace, or in all of them if namespace is "".
func listPath(namespace, kind string) string {
	if namespace == "" {
		return "/api/" + kind
	}
	return "/api/namespaces/" + url.PathEscape(namespace) + "/" + kind
}

// objectPath returns the URL path of one namespaced object. PathEscape makes
// sure a strange name can't change the meaning of the URL.
func objectPath(namespace, kind, name string) string {
	return listPath(namespace, kind) + "/" + url.PathEscape(name)
}

// ListPods fetches the pods in a namespace, or in all namespaces if
// namespace is "".
func (c *Client) ListPods(namespace string) ([]api.Pod, error) {
	var pods []api.Pod
	err := c.get(listPath(namespace, "pods"), &pods)
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	return pods, nil
}

// GetPod fetches one pod.
func (c *Client) GetPod(namespace, name string) (api.Pod, error) {
	var pod api.Pod
	err := c.get(objectPath(namespace, "pods", name), &pod)
	if err != nil {
		return api.Pod{}, fmt.Errorf("get pod %q: %w", name, err)
	}
	return pod, nil
}

// CreatePod sends a new pod to the API server, in pod.Namespace.
func (c *Client) CreatePod(pod api.Pod) error {
	err := c.send(http.MethodPost, listPath(pod.Namespace, "pods"), pod, http.StatusCreated)
	if err != nil {
		return fmt.Errorf("create pod: %w", err)
	}
	return nil
}

// DeletePod removes a pod.
func (c *Client) DeletePod(namespace, name string) error {
	err := c.send(http.MethodDelete, objectPath(namespace, "pods", name), nil, http.StatusOK)
	if err != nil {
		return fmt.Errorf("delete pod %q: %w", name, err)
	}
	return nil
}

// BindPod asks the API server to assign a pod to a node.
func (c *Client) BindPod(namespace, name, nodeName string) error {
	err := c.send(http.MethodPost, objectPath(namespace, "pods", name)+"/binding", api.Binding{NodeName: nodeName}, http.StatusOK)
	if err != nil {
		return fmt.Errorf("bind pod %q: %w", name, err)
	}
	return nil
}

// SetPodPhase reports a pod's new phase to the API server.
func (c *Client) SetPodPhase(namespace, name string, phase api.PodPhase) error {
	return c.SetPodStatus(namespace, name, api.PodStatus{Phase: phase})
}

// SetPodStatus reports a pod's new phase, readiness and address.
func (c *Client) SetPodStatus(namespace, name string, status api.PodStatus) error {
	err := c.send(http.MethodPost, objectPath(namespace, "pods", name)+"/status", status, http.StatusOK)
	if err != nil {
		return fmt.Errorf("set status of pod %q: %w", name, err)
	}
	return nil
}

// WatchPods opens a watch on the pods of every namespace. Events arrive on
// the returned channel until ctx is cancelled or the connection breaks, then
// it is closed.
func (c *Client) WatchPods(ctx context.Context) (<-chan api.PodEvent, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/watch/pods", nil)
	if err != nil {
		return nil, fmt.Errorf("watch pods: %w", err)
	}

	// No timeout here: a watch is meant to stay open for a long time.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("watch pods: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, fmt.Errorf("watch pods: %w", statusError(resp))
	}

	events := make(chan api.PodEvent)

	go func() {
		defer close(events)
		defer resp.Body.Close()

		dec := json.NewDecoder(resp.Body)
		for {
			var event api.PodEvent
			err := dec.Decode(&event)
			if err != nil {
				return
			}

			select {
			case events <- event:
			case <-ctx.Done():
				return
			}
		}
	}()

	return events, nil
}

// ListNodes fetches every node.
func (c *Client) ListNodes() ([]api.Node, error) {
	var nodes []api.Node
	err := c.get("/api/nodes", &nodes)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	return nodes, nil
}

// CreateNode sends a new node to the API server.
func (c *Client) CreateNode(node api.Node) error {
	err := c.send(http.MethodPost, "/api/nodes", node, http.StatusCreated)
	if err != nil {
		return fmt.Errorf("create node: %w", err)
	}
	return nil
}

// PutNode creates or replaces a node. Kubelets use it to register their node
// and to send heartbeats.
func (c *Client) PutNode(node api.Node) error {
	err := c.send(http.MethodPut, "/api/nodes/"+url.PathEscape(node.Name), node, http.StatusOK)
	if err != nil {
		return fmt.Errorf("put node %q: %w", node.Name, err)
	}
	return nil
}

// SetNodeReady marks a node Ready or NotReady.
func (c *Client) SetNodeReady(name string, ready bool) error {
	err := c.send(http.MethodPost, "/api/nodes/"+url.PathEscape(name)+"/status", api.NodeStatus{Ready: ready}, http.StatusOK)
	if err != nil {
		return fmt.Errorf("set node %q ready: %w", name, err)
	}
	return nil
}

// get fetches path from the API server and decodes the JSON answer into out,
// which must be a pointer.
func (c *Client) get(path string, out any) error {
	resp, err := c.http.Get(c.baseURL + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}

	err = json.NewDecoder(resp.Body).Decode(out)
	if err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// send makes a request to the API server with in as its JSON body (nil for
// no body), and checks that the answer has the status code want.
func (c *Client) send(method, path string, in any, want int) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("encode: %w", err)
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != want {
		return statusError(resp)
	}
	return nil
}

// ErrNotFound and ErrConflict are wrapped into the errors of requests the
// server answered with 404 or 409, so callers can check for them with errors.Is.
var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

// serverError is an unexpected answer from the API server.
type serverError struct {
	message string // what the server said, with its status
	kind    error  // ErrNotFound, ErrConflict, or nil
}

// Error makes serverError an error: this is the message that gets printed.
func (e *serverError) Error() string {
	return e.message
}

// Unwrap lets errors.Is look inside: errors.Is(err, ErrNotFound) asks
// Unwrap for the error underneath and compares that.
func (e *serverError) Unwrap() error {
	return e.kind
}

// statusError turns an unexpected answer from the server into an error that
// includes the server's message.
func statusError(resp *http.Response) error {
	msg, _ := io.ReadAll(resp.Body)
	err := &serverError{message: fmt.Sprintf("server returned %s: %s", resp.Status, strings.TrimSpace(string(msg)))}

	switch resp.StatusCode {
	case http.StatusNotFound:
		err.kind = ErrNotFound
	case http.StatusConflict:
		err.kind = ErrConflict
	}
	return err
}
