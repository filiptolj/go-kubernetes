// Package proxy makes Services reachable, like kube-proxy in real
// Kubernetes. For every Service it listens on the Service's port, and
// forwards each connection to one of the running pods the Service selects,
// taking turns.
package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// Proxy forwards connections for every Service.
type Proxy struct {
	Client   *client.Client
	Every    time.Duration // how often to look for new Services and pods
	BindAddr string        // where to listen, such as "127.0.0.1"

	mu       sync.Mutex
	services map[string]*service // by the Service's "namespace/name"
}

// service is one Service the proxy listens for.
type service struct {
	svc       api.Service
	listener  net.Listener
	endpoints []endpoint // the pods to forward to; guarded by Proxy.mu
	next      int        // which endpoint gets the next connection
}

// endpoint is a pod a Service forwards to.
type endpoint struct {
	Pod     string
	Address string
}

// Run keeps the proxy in sync with the Services and pods every p.Every,
// until ctx is cancelled. Then it stops listening.
func (p *Proxy) Run(ctx context.Context) {
	ticker := time.NewTicker(p.Every)
	defer ticker.Stop()

	p.sync()
	for {
		select {
		case <-ctx.Done():
			p.closeAll()
			return
		case <-ticker.C:
			p.sync()
		}
	}
}

// sync starts listening for new Services, stops for deleted ones, and
// refreshes the pods each Service forwards to.
func (p *Proxy) sync() {
	services, err := p.Client.ListServices("") // every namespace
	if err != nil {
		log.Printf("proxy: %v", err)
		return
	}

	pods, err := p.Client.ListPods("")
	if err != nil {
		log.Printf("proxy: %v", err)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.services == nil {
		p.services = make(map[string]*service)
	}

	wanted := make(map[string]bool)
	for _, svc := range services {
		key := api.Key(svc.Namespace, svc.Name)
		wanted[key] = true

		s, ok := p.services[key]
		if ok && s.svc.Port != svc.Port {
			s.listener.Close() // the port changed: listen again below
			ok = false
		}
		if !ok {
			l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", p.BindAddr, svc.Port))
			if err != nil {
				log.Printf("proxy: service %s: %v", key, err)
				continue
			}
			s = &service{svc: svc, listener: l}
			p.services[key] = s
			go p.serve(s)
			log.Printf("service %s: listening on %s", key, l.Addr())
		}
		s.svc = svc

		endpoints := endpointsFor(svc, pods)
		if !slices.Equal(endpoints, s.endpoints) {
			log.Printf("service %s: %d pods to forward to", key, len(endpoints))
			s.endpoints = endpoints
		}
	}

	for key, s := range p.services {
		if !wanted[key] {
			s.listener.Close()
			delete(p.services, key)
			log.Printf("service %s: deleted, stopped listening", key)
		}
	}
}

// endpointsFor returns the ready pods a Service selects: in its own
// namespace, with matching labels.
func endpointsFor(svc api.Service, pods []api.Pod) []endpoint {
	var endpoints []endpoint
	for _, pod := range pods {
		if pod.Namespace == svc.Namespace && pod.Phase == api.PodRunning && pod.Ready &&
			pod.Address != "" && pod.Labels.Matches(svc.Selector) {
			endpoints = append(endpoints, endpoint{Pod: pod.Name, Address: pod.Address})
		}
	}
	return endpoints
}

// serve accepts connections for a Service until its listener is closed.
func (p *Proxy) serve(s *service) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return // the listener was closed
		}
		go p.forward(s, conn)
	}
}

// forward connects a client to one of the Service's pods and copies data
// both ways until either side hangs up.
func (p *Proxy) forward(s *service, conn net.Conn) {
	defer conn.Close()

	ep, ok := p.pick(s)
	if !ok {
		log.Printf("service %s: no ready pods to forward to", api.Key(s.svc.Namespace, s.svc.Name))
		return
	}

	backend, err := net.DialTimeout("tcp", ep.Address, 3*time.Second)
	if err != nil {
		log.Printf("service %s: pod %q: %v", api.Key(s.svc.Namespace, s.svc.Name), ep.Pod, err)
		return
	}
	defer backend.Close()

	log.Printf("service %s: connection from %s -> pod %q", api.Key(s.svc.Namespace, s.svc.Name), conn.RemoteAddr(), ep.Pod)

	// Copy in both directions at the same time. When one direction ends,
	// returning closes both connections, which ends the other copy too.
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(backend, conn)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(conn, backend)
		done <- struct{}{}
	}()
	<-done
}

// pick chooses the pod for the next connection, taking turns.
func (p *Proxy) pick(s *service) (endpoint, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(s.endpoints) == 0 {
		return endpoint{}, false
	}
	ep := s.endpoints[s.next%len(s.endpoints)]
	s.next++
	return ep, true
}

// closeAll stops listening for every Service.
func (p *Proxy) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, s := range p.services {
		s.listener.Close()
		delete(p.services, key)
	}
}
