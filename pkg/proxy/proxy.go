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
	"strconv"
	"sync"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// Proxy forwards connections for every Service.
type Proxy struct {
	Client   *client.Client
	Every    time.Duration // how often to look again even if nothing changed
	BindAddr string        // where to listen, such as "127.0.0.1"

	// IngressAddr is where to serve Ingresses, such as "127.0.0.1:8090";
	// "" for nowhere.
	IngressAddr string

	// PodIPs makes the proxy send connections to the pods' own addresses on
	// the cluster network, instead of to where their ports are published
	// on this machine. The proxy that runs inside the cluster network uses it.
	PodIPs bool

	// Where to read Services, pods and Ingresses. If nil, the proxy asks
	// the API server.
	Services  *client.Informer[api.Service]
	Pods      *client.Informer[api.Pod]
	Ingresses *client.Informer[api.Ingress]

	mu       sync.Mutex
	services map[string]*service // by "namespace/name:port"
}

// service is one port of a Service, which the proxy listens on.
type service struct {
	svc       api.Service
	port      api.ServicePort
	listener  net.Listener
	endpoints []endpoint // the pods to forward to; guarded by Proxy.mu
	next      int        // which endpoint gets the next connection
}

// endpoint is a pod a Service forwards to.
type endpoint struct {
	Pod     string
	Address string
}

// Run keeps the proxy in sync with the Services and pods, whenever they
// change, until ctx is cancelled. Then it stops listening.
func (p *Proxy) Run(ctx context.Context) {
	if p.IngressAddr != "" {
		go p.serveIngress(ctx, p.IngressAddr)
	}
	client.RunOnChange(ctx, "proxy", p.Every, p.sync, p.Services, p.Pods)
	p.closeAll()
}

// list returns every Service and every pod.
func (p *Proxy) list() ([]api.Service, []api.Pod, error) {
	if p.Services != nil && p.Pods != nil {
		return p.Services.List(""), p.Pods.List(""), nil
	}
	services, err := p.Client.ListServices("") // every namespace
	if err != nil {
		return nil, nil, err
	}
	pods, err := p.Client.ListPods("")
	return services, pods, err
}

// sync starts listening for new Services, stops for deleted ones, and
// refreshes the pods each Service forwards to.
func (p *Proxy) sync() {
	services, pods, err := p.list()
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
		for _, port := range svc.Ports {
			key := fmt.Sprintf("%s:%d", api.Key(svc.Namespace, svc.Name), port.Port)
			wanted[key] = true

			s, ok := p.services[key]
			if !ok {
				l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", p.BindAddr, port.Port))
				if err != nil {
					log.Printf("proxy: service %s: %v", key, err)
					continue
				}
				s = &service{svc: svc, port: port, listener: l}
				p.services[key] = s
				go p.serve(s)
				log.Printf("service %s: listening on %s", key, l.Addr())
			}
			s.svc, s.port = svc, port

			endpoints := endpointsFor(svc, port, pods, p.PodIPs)
			if !slices.Equal(endpoints, s.endpoints) {
				log.Printf("service %s: %d pods to forward to", key, len(endpoints))
				s.endpoints = endpoints
			}
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

// endpointsFor returns where to send connections to one port of a Service:
// the target port of every ready pod it selects, in its own namespace, with
// matching labels. With podIPs, that is the pod's IP and the target port;
// otherwise, where the target port is published on this machine.
func endpointsFor(svc api.Service, port api.ServicePort, pods []api.Pod, podIPs bool) []endpoint {
	var endpoints []endpoint
	for _, pod := range pods {
		if pod.Namespace != svc.Namespace || pod.Phase != api.PodRunning || !pod.Ready || !pod.Labels.Matches(svc.Selector) {
			continue
		}
		address := pod.HostPorts[port.Target()]
		if podIPs {
			address = ""
			if pod.PodIP != "" {
				address = net.JoinHostPort(pod.PodIP, strconv.Itoa(port.Target()))
			}
		}
		if address != "" {
			endpoints = append(endpoints, endpoint{Pod: pod.Name, Address: address})
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
		log.Printf("service %s: no ready pods to forward to", fmt.Sprintf("%s:%d", api.Key(s.svc.Namespace, s.svc.Name), s.port.Port))
		return
	}

	backend, err := net.DialTimeout("tcp", ep.Address, 3*time.Second)
	if err != nil {
		log.Printf("service %s: pod %q: %v", fmt.Sprintf("%s:%d", api.Key(s.svc.Namespace, s.svc.Name), s.port.Port), ep.Pod, err)
		return
	}
	defer backend.Close()

	log.Printf("service %s: connection from %s -> pod %q", fmt.Sprintf("%s:%d", api.Key(s.svc.Namespace, s.svc.Name), s.port.Port), conn.RemoteAddr(), ep.Pod)

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
