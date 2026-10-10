package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// route finds where an HTTP request for host and path goes: the backend of
// the best matching Ingress path, and the namespace of its Ingress. Rules
// for the request's host win over rules for any host; among those, the
// longest matching path wins, as in Kubernetes.
//
// A Prefix path matches whole path segments: "/api" matches "/api" and
// "/api/users", but not "/apis". An Exact path matches only itself.
func route(ingresses []api.Ingress, host, path string) (backend api.IngressServiceBackend, namespace string, ok bool) {
	host = strings.ToLower(host)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h // "shop.local:8090" is host "shop.local"
	}

	for _, wantHost := range []string{host, ""} { // first this host, then any host
		best := -1
		for _, ing := range ingresses {
			for _, rule := range ing.Rules {
				if strings.ToLower(rule.Host) != wantHost {
					continue
				}
				for _, p := range rule.HTTP.Paths {
					if matches(p, path) && len(p.Path) > best {
						best = len(p.Path)
						backend, namespace, ok = p.Backend.Service, ing.Namespace, true
					}
				}
			}
		}
		if ok {
			return backend, namespace, true
		}
	}
	return backend, "", false
}

// matches reports whether a request path is matched by an Ingress path.
func matches(p api.IngressPath, path string) bool {
	if p.PathType == "Exact" {
		return path == p.Path
	}
	prefix := strings.TrimSuffix(p.Path, "/")
	return prefix == "" || path == prefix || strings.HasPrefix(path, prefix+"/")
}

// serveIngress answers HTTP requests on addr by passing each one to the
// Service an Ingress routes it to, until ctx is cancelled.
func (p *Proxy) serveIngress(ctx context.Context, addr string) {
	srv := &http.Server{Addr: addr, Handler: http.HandlerFunc(p.handleIngress), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	log.Printf("ingress: listening on %s", addr)
	err := srv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("ingress: %v", err)
	}
}

// handleIngress sends one request on to a pod of the Service its Ingress
// rule names, and the pod's answer back.
func (p *Proxy) handleIngress(w http.ResponseWriter, r *http.Request) {
	ingresses, err := p.listIngresses()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	backend, namespace, ok := route(ingresses, r.Host, r.URL.Path)
	if !ok {
		http.Error(w, fmt.Sprintf("no ingress rule for %s%s", r.Host, r.URL.Path), http.StatusNotFound)
		return
	}

	key := fmt.Sprintf("%s:%d", api.Key(namespace, backend.Name), backend.Port.Number)
	p.mu.Lock()
	s, ok := p.services[key]
	p.mu.Unlock()
	if !ok {
		http.Error(w, fmt.Sprintf("service %s doesn't exist", key), http.StatusServiceUnavailable)
		return
	}
	ep, ok := p.pick(s)
	if !ok {
		http.Error(w, fmt.Sprintf("service %s has no ready pods", key), http.StatusServiceUnavailable)
		return
	}

	// ReverseProxy sends the request on and copies the answer back. Rewrite
	// points it at the pod, adds the X-Forwarded-For/Host/Proto headers, and
	// keeps the original Host, which apps behind an ingress often look at.
	rp := &httputil.ReverseProxy{Rewrite: func(pr *httputil.ProxyRequest) {
		pr.SetURL(&url.URL{Scheme: "http", Host: ep.Address})
		pr.SetXForwarded()
		pr.Out.Host = pr.In.Host
	}}
	rp.ServeHTTP(w, r)
}

// listIngresses returns every Ingress, from the informer if there is one.
func (p *Proxy) listIngresses() ([]api.Ingress, error) {
	if p.Ingresses != nil {
		return p.Ingresses.List(""), nil
	}
	return p.Client.Ingresses().List("")
}
