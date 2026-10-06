package kubelet

import (
	"errors"
	"log"
	"net"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// probeEvery is how often waitUntilReady checks a starting pod.
const probeEvery = 500 * time.Millisecond

// waitUntilReady checks the pod's address until something answers there,
// then reports the pod ready. It gives up if the pod stops running here.
func (k *Kubelet) waitUntilReady(pod api.Pod, address string) {
	key := api.Key(pod.Namespace, pod.Name)
	for k.isRunning(key) {
		if probe(address) {
			err := k.client.SetPodStatus(pod.Namespace, pod.Name, api.PodStatus{Phase: api.PodRunning, Ready: true, Address: address})
			if err != nil {
				log.Printf("could not report pod %s as ready: %v", key, err)
				return
			}
			log.Printf("pod %s is ready", key)
			k.events.Normal("Pod", pod.Namespace, pod.Name, "Ready", "answering on %s", address)
			return
		}
		time.Sleep(probeEvery)
	}
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
