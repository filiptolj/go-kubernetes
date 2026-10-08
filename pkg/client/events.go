package client

import (
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// RecordEvent sends an event to the API server.
func (c *Client) RecordEvent(e api.Event) error {
	err := c.send(http.MethodPost, "/api/v1/events", e, http.StatusCreated)
	if err != nil {
		return fmt.Errorf("record event: %w", err)
	}
	return nil
}

// ListEvents fetches events, oldest first. An empty namespace, kind or name
// matches every one.
func (c *Client) ListEvents(namespace, kind, name string) ([]api.Event, error) {
	// url.Values builds a query string such as "kind=Pod&name=web-x7k2p",
	// escaping any characters that aren't allowed in a URL.
	query := url.Values{"namespace": {namespace}, "kind": {kind}, "name": {name}}

	var events []api.Event
	err := c.get("/api/v1/events?"+query.Encode(), &events)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	return events, nil
}

// Recorder records events on behalf of one component, such as "scheduler".
type Recorder struct {
	client *Client
	source string
}

// Recorder returns a Recorder for the component called source.
func (c *Client) Recorder(source string) *Recorder {
	return &Recorder{client: c, source: source}
}

// Normal records that something expected happened to an object. namespace
// is "" for objects that don't live in one, such as nodes. The message is
// built from format and args, like fmt.Printf.
func (r *Recorder) Normal(kind, namespace, name, reason, format string, args ...any) {
	r.record(api.EventNormal, kind, namespace, name, reason, fmt.Sprintf(format, args...))
}

// Warning records that something went wrong with an object.
func (r *Recorder) Warning(kind, namespace, name, reason, format string, args ...any) {
	r.record(api.EventWarning, kind, namespace, name, reason, fmt.Sprintf(format, args...))
}

// record sends one event. A failure is only logged: losing an event must
// never stop a component from doing its real job.
func (r *Recorder) record(eventType, kind, namespace, name, reason, message string) {
	err := r.client.RecordEvent(api.Event{
		Kind:      kind,
		Namespace: namespace,
		Name:      name,
		Type:      eventType,
		Reason:    reason,
		Message:   message,
		Source:    r.source,
	})
	if err != nil {
		log.Printf("%v", err)
	}
}
