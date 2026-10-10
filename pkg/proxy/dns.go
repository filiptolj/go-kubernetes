package proxy

import (
	"context"
	"log"
	"net"
	"strings"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// DNS is the cluster's DNS server, like CoreDNS in Kubernetes. It answers
// for <service>.<namespace>.svc.<domain> with the address where the
// cluster's Services can be reached, and passes every other name on to an
// upstream server.
type DNS struct {
	Domain    string // the cluster's domain, such as "cluster.local"
	ServiceIP net.IP // the answer for every Service: where the proxy listens
	Upstream  string // where other names go, as host:port

	// Exists reports whether a Service exists.
	Exists func(namespace, name string) bool
}

// dnsTTL is how many seconds clients may remember an answer. Short, so a
// deleted Service stops resolving soon.
const dnsTTL = 5

// Serve answers DNS questions sent over UDP to addr, such as ":53", until
// ctx is cancelled.
func (d *DNS) Serve(ctx context.Context, addr string) error {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	log.Printf("dns: answering for *.svc.%s on %s", d.Domain, conn.LocalAddr())

	buf := make([]byte, 1500)
	for {
		n, from, err := conn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		query := append([]byte(nil), buf[:n]...)
		go func() {
			reply := d.answer(query)
			if reply == nil {
				reply = d.forward(query)
			}
			if reply != nil {
				conn.WriteTo(reply, from)
			}
		}()
	}
}

// answer answers a question about a name in the cluster's domain. It
// returns nil for other names, and for anything it can't read.
func (d *DNS) answer(query []byte) []byte {
	var p dnsmessage.Parser
	header, err := p.Start(query)
	if err != nil {
		return nil
	}
	q, err := p.Question()
	if err != nil {
		return nil
	}

	name := strings.ToLower(strings.TrimSuffix(q.Name.String(), "."))
	if name != d.Domain && !strings.HasSuffix(name, "."+d.Domain) {
		return nil
	}

	reply := dnsmessage.Header{ID: header.ID, Response: true, Authoritative: true, RecursionDesired: header.RecursionDesired}
	var answers []dnsmessage.Resource

	// <service>.<namespace>.svc.<domain>
	service, namespace, ok := serviceName(name, d.Domain)
	switch {
	case !ok || !d.Exists(namespace, service):
		reply.RCode = dnsmessage.RCodeNameError // no such name
	case q.Type == dnsmessage.TypeA:
		var ip [4]byte
		copy(ip[:], d.ServiceIP.To4())
		answers = append(answers, dnsmessage.Resource{
			Header: dnsmessage.ResourceHeader{Name: q.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: dnsTTL},
			Body:   &dnsmessage.AResource{A: ip},
		})
	default:
		// The name exists, but has no address of the kind asked for (an
		// IPv6 one, say): answer with nothing, and no error.
	}

	msg := dnsmessage.Message{Header: reply, Questions: []dnsmessage.Question{q}, Answers: answers}
	data, err := msg.Pack()
	if err != nil {
		return nil
	}
	return data
}

// serviceName splits "web.shop.svc.cluster.local" into "web" and "shop".
func serviceName(name, domain string) (service, namespace string, ok bool) {
	rest, ok := strings.CutSuffix(name, ".svc."+domain)
	if !ok {
		return "", "", false
	}
	service, namespace, ok = strings.Cut(rest, ".")
	if !ok || service == "" || namespace == "" || strings.Contains(namespace, ".") {
		return "", "", false
	}
	return service, namespace, true
}

// forward sends a question to the upstream server and returns its reply,
// or nil if it doesn't answer in time.
func (d *DNS) forward(query []byte) []byte {
	conn, err := net.Dial("udp", d.Upstream)
	if err != nil {
		return nil
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(3 * time.Second))
	_, err = conn.Write(query)
	if err != nil {
		return nil
	}
	buf := make([]byte, 1500)
	n, err := conn.Read(buf)
	if err != nil {
		return nil
	}
	return buf[:n]
}
