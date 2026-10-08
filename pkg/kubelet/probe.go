package kubelet

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// probeEvery is how often the readiness of containers without a readiness
// probe is checked.
const probeEvery = 500 * time.Millisecond

// probeSettings returns a probe's settings, with the defaults filled in.
func probeSettings(p *api.Probe) (initialDelay, period time.Duration, threshold int) {
	initialDelay = time.Duration(p.InitialDelaySeconds) * time.Second
	period = 10 * time.Second
	if p.PeriodSeconds > 0 {
		period = time.Duration(p.PeriodSeconds) * time.Second
	}
	threshold = 3
	if p.FailureThreshold > 0 {
		threshold = p.FailureThreshold
	}
	return initialDelay, period, threshold
}

// check runs a probe against a container's address. A nil probe, or one
// without httpGet, only checks that something answers on the port.
func check(p *api.Probe, address string) bool {
	if address == "" {
		return false
	}
	if p != nil && p.HTTPGet != nil {
		return checkHTTP(address, p.HTTPGet.Path)
	}
	return probe(address)
}

// checkHTTP sends a GET request; a status from 200 to 399 means healthy.
func checkHTTP(address, path string) bool {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	client := http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + address + path)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

// probe reports whether a program is listening at address.
//
// Connecting isn't enough: Docker accepts connections on a published port
// even before the program inside the container listens, and then closes
// them at once. So after connecting we wait briefly for data. A program
// that is really there either keeps the connection open, waiting for us to
// speak (like a web server), or greets us (like a mail server). A connection
// that is closed straight away means nobody is home yet.
func probe(address string) bool {
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	_, err = conn.Read(make([]byte, 1))

	// errors.As checks whether err is (or wraps) a net.Error, and if so puts
	// it in netErr, so we can ask it whether it was a timeout.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true // still open and waiting for us: ready
	}
	return err == nil // it sent us something: ready
}
