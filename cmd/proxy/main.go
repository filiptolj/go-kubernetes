package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/proxy"
)

// The proxy runs in two places. On the machine, it makes Services reachable
// on 127.0.0.1 and serves Ingresses. Inside the cluster network (started by
// minik8s as a container, with -pod-ips and -dns), it makes Services
// reachable from pods, by name, through the cluster DNS it also serves.
func main() {
	server := flag.String("server", "http://localhost:8080", "API server address")
	bind := flag.String("bind", "127.0.0.1", "address to listen on for services (0.0.0.0 for every network interface)")
	podIPs := flag.Bool("pod-ips", false, "send connections to the pods' cluster IPs, not to their ports published on this machine")
	ingressAddr := flag.String("ingress-addr", "", "serve Ingresses here, such as 127.0.0.1:8090 (empty: don't)")
	dnsAddr := flag.String("dns", "", "serve the cluster DNS here, such as :53 (empty: don't)")
	dnsIP := flag.String("dns-service-ip", "", "the address DNS gives for every Service: where this proxy can be reached")
	upstream := flag.String("dns-upstream", "127.0.0.11:53", "where DNS sends names outside the cluster (Docker's own DNS server by default)")
	domain := flag.String("domain", "cluster.local", "the cluster's DNS domain")
	flag.Parse()

	// SIGTERM too: that's what `docker stop` sends when it runs in a container.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	c := client.New(*server)
	p := &proxy.Proxy{
		Client:      c,
		Every:       10 * time.Second,
		BindAddr:    *bind,
		PodIPs:      *podIPs,
		IngressAddr: *ingressAddr,
		Services:    client.NewInformer[api.Service](c, "services"),
		Pods:        client.NewInformer[api.Pod](c, "pods"),
	}
	if *ingressAddr != "" {
		p.Ingresses = client.NewInformer[api.Ingress](c, "ingresses")
	}

	log.Printf("proxy started")
	var wg sync.WaitGroup
	wg.Go(func() { p.Services.Run(ctx) })
	wg.Go(func() { p.Pods.Run(ctx) })
	if p.Ingresses != nil {
		wg.Go(func() { p.Ingresses.Run(ctx) })
	}
	if *dnsAddr != "" {
		ip := net.ParseIP(*dnsIP)
		if ip == nil || ip.To4() == nil {
			log.Fatalf("-dns needs -dns-service-ip, an IPv4 address, not %q", *dnsIP)
		}
		dns := &proxy.DNS{
			Domain:    *domain,
			ServiceIP: ip,
			Upstream:  *upstream,
			Exists: func(namespace, name string) bool {
				_, ok := p.Services.Get(namespace, name)
				return ok
			},
		}
		wg.Go(func() {
			err := dns.Serve(ctx, *dnsAddr)
			if err != nil {
				log.Printf("dns: %v", err)
			}
		})
	}
	wg.Go(func() { p.Run(ctx) })
	wg.Wait()
	log.Printf("proxy stopped")
}
