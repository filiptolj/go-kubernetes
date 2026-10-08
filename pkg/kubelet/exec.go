package kubelet

import (
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// handleExec runs a command inside a running container:
//
//	POST /exec/{namespace}/{pod}/{container}?command=ls&command=-l
//
// Each command parameter is one word of the command. With ?stdin=true, the
// request body is the command's input. The command's output is sent back
// as it comes. Its exit code can only be known at the end, after the output,
// when the headers have long been sent, so it comes in a trailer: a header
// sent after the body, called X-Exit-Code.
func (k *Kubelet) handleExec(w http.ResponseWriter, r *http.Request) {
	namespace, podName, name := r.PathValue("namespace"), r.PathValue("pod"), r.PathValue("container")
	command := r.URL.Query()["command"]
	if len(command) == 0 {
		http.Error(w, "no command: add ?command=...", http.StatusBadRequest)
		return
	}

	k.mu.Lock()
	rp, ok := k.pods[api.Key(namespace, podName)]
	var up bool
	if ok && rp.containers[name] != nil {
		up = rp.containers[name].up
	}
	k.mu.Unlock()
	if !ok {
		http.Error(w, fmt.Sprintf("pod %q isn't running on this node", podName), http.StatusNotFound)
		return
	}
	i := slices.IndexFunc(slices.Concat(rp.pod.InitContainers, rp.pod.Containers), func(c api.Container) bool { return c.Name == name })
	if i < 0 {
		http.Error(w, fmt.Sprintf("pod %q has no container %q", podName, name), http.StatusNotFound)
		return
	}
	if !up {
		http.Error(w, fmt.Sprintf("container %q isn't running", name), http.StatusConflict)
		return
	}
	c := slices.Concat(rp.pod.InitContainers, rp.pod.Containers)[i]

	// Full duplex lets the handler keep reading the request body (the
	// command's input) after it has started writing the answer.
	rc := http.NewResponseController(w)
	var stdin io.Reader
	if r.URL.Query().Get("stdin") == "true" {
		rc.EnableFullDuplex()
		stdin = r.Body
	}

	w.Header().Set("Trailer", "X-Exit-Code")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	out := &flushWriter{w: w, rc: rc}
	code, err := k.runtime.Exec(r.Context(), rp.pod, c, command, stdin, out)
	if err != nil {
		fmt.Fprintf(out, "exec: %v\n", err)
		code = 126 // what shells use for "found but couldn't run"
	}
	w.Header().Set("X-Exit-Code", strconv.Itoa(code))
}

// flushWriter sends everything written to it to the client straight away,
// so output appears as the command produces it.
type flushWriter struct {
	w  io.Writer
	rc *http.ResponseController
}

func (f *flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	f.rc.Flush()
	return n, err
}
