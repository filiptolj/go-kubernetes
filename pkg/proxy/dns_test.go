package proxy

import (
	"context"
	"net"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// ask sends one DNS question to addr and returns the reply.
func ask(t *testing.T, addr, name string, qtype dnsmessage.Type) dnsmessage.Message {
	t.Helper()

	q := dnsmessage.Message{
		Header:    dnsmessage.Header{ID: 42, RecursionDesired: true},
		Questions: []dnsmessage.Question{{Name: dnsmessage.MustNewName(name), Type: qtype, Class: dnsmessage.ClassINET}},
	}
	data, _ := q.Pack()

	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	conn.Write(data)
	buf := make([]byte, 1500)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("no reply for %s: %v", name, err)
	}

	var reply dnsmessage.Message
	if err := reply.Unpack(buf[:n]); err != nil {
		t.Fatal(err)
	}
	if reply.ID != 42 {
		t.Errorf("reply has ID %d, want the question's 42", reply.ID)
	}
	return reply
}

// freeUDP returns a UDP address nobody is using.
func freeUDP(t *testing.T) string {
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().String()
}

func TestDNS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A pretend upstream server, which answers every <a>.<b>.svc.example.com
	// with 1.2.3.4.
	upstream := &DNS{Domain: "example.com", ServiceIP: net.IPv4(1, 2, 3, 4), Exists: func(string, string) bool { return true }}
	upstreamAddr := freeUDP(t)
	go upstream.Serve(ctx, upstreamAddr)

	d := &DNS{
		Domain:    "cluster.local",
		ServiceIP: net.IPv4(10, 244, 0, 2),
		Upstream:  upstreamAddr,
		Exists:    func(namespace, name string) bool { return namespace == "shop" && name == "web" },
	}
	addr := freeUDP(t)
	go d.Serve(ctx, addr)
	time.Sleep(50 * time.Millisecond)

	reply := ask(t, addr, "web.shop.svc.cluster.local.", dnsmessage.TypeA)
	if reply.RCode != dnsmessage.RCodeSuccess || len(reply.Answers) != 1 ||
		reply.Answers[0].Body.(*dnsmessage.AResource).A != [4]byte{10, 244, 0, 2} {
		t.Errorf("web.shop: got %v %v, want 10.244.0.2", reply.RCode, reply.Answers)
	}

	if reply := ask(t, addr, "WEB.shop.svc.cluster.local.", dnsmessage.TypeAAAA); reply.RCode != dnsmessage.RCodeSuccess || len(reply.Answers) != 0 {
		t.Errorf("AAAA for an existing Service: got %v with %d answers, want success with none", reply.RCode, len(reply.Answers))
	}

	for _, name := range []string{"nope.shop.svc.cluster.local.", "web.default.svc.cluster.local.", "web.cluster.local."} {
		if reply := ask(t, addr, name, dnsmessage.TypeA); reply.RCode != dnsmessage.RCodeNameError {
			t.Errorf("%s: got %v, want NXDOMAIN", name, reply.RCode)
		}
	}

	// Other names go upstream.
	reply = ask(t, addr, "www.a.svc.example.com.", dnsmessage.TypeA)
	if len(reply.Answers) != 1 || reply.Answers[0].Body.(*dnsmessage.AResource).A != [4]byte{1, 2, 3, 4} {
		t.Errorf("a name outside the cluster: got %v, want the upstream's answer", reply.Answers)
	}
}
