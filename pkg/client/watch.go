package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// Watch opens a watch on the objects at a list path, such as
// /apis/apps/v1/deployments for the Deployments of every namespace. Events
// arrive on the returned channel until ctx is cancelled or the connection
// breaks; then it is closed.
//
// Watch is a function, not a method, because Go methods can't have type
// parameters of their own.
func Watch[T any](ctx context.Context, c *Client, path string) (<-chan api.WatchEvent[T], error) {
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path+sep+"watch=true", nil)
	if err != nil {
		return nil, fmt.Errorf("watch %s: %w", path, err)
	}

	// No timeout here: a watch is meant to stay open for a long time.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("watch %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, fmt.Errorf("watch %s: %w", path, statusError(resp))
	}

	events := make(chan api.WatchEvent[T])
	go func() {
		defer close(events)
		defer resp.Body.Close()

		dec := json.NewDecoder(resp.Body)
		for {
			var event api.WatchEvent[T]
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

// selectorPath returns the list path of one kind (plural, as in URLs) in a
// namespace, or in all of them if namespace is "", asking only for objects
// that match a label selector, unless selector is "".
func selectorPath(plural, namespace, selector string) string {
	path := listPath(namespace, plural)
	if selector != "" {
		path += "?labelSelector=" + url.QueryEscape(selector)
	}
	return path
}

// List fetches the objects of one kind whose labels match selector, such as
// List[api.Pod](c, "pods", "default", "app=web"). namespace "" means all
// namespaces, and selector "" all objects.
func List[T any](c *Client, plural, namespace, selector string) ([]T, error) {
	var list []T
	err := c.get(selectorPath(plural, namespace, selector), &list)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", plural, err)
	}
	return list, nil
}

// WatchList watches the objects that List would return. See Watch.
func WatchList[T any](ctx context.Context, c *Client, plural, namespace, selector string) (<-chan api.WatchEvent[T], error) {
	return Watch[T](ctx, c, selectorPath(plural, namespace, selector))
}
