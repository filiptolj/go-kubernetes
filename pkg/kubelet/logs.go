package kubelet

import (
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// logsHandler serves container logs: GET /logs/{namespace}/{pod}/{container},
// with ?follow=true to keep sending new output as it is written.
func (k *Kubelet) logsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /logs/{namespace}/{pod}/{container}", k.handleLogs)
	return mux
}

// handleLogs sends a container's log file. With ?follow=true it keeps the
// connection open and sends new output as it is written, like `tail -f`,
// until the pod stops or the client disconnects.
func (k *Kubelet) handleLogs(w http.ResponseWriter, r *http.Request) {
	namespace, pod, container := r.PathValue("namespace"), r.PathValue("pod"), r.PathValue("container")

	// The names become part of a file path. Without this check, a request for
	// pod ".." could read files outside the log folder.
	if !safeName(namespace) || !safeName(pod) || !safeName(container) {
		http.Error(w, "invalid namespace, pod or container name", http.StatusBadRequest)
		return
	}

	f, err := os.Open(filepath.Join(k.logDir(namespace, pod), container+".log"))
	if errors.Is(err, fs.ErrNotExist) {
		http.Error(w, "no logs for container "+container+" of pod "+pod+" on this node", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, err = io.Copy(w, f)
	if err != nil || r.URL.Query().Get("follow") != "true" {
		return
	}

	// Follow: every half second, send whatever was added to the file. The
	// file remembers how far we've read, so each Copy continues from there.
	rc := http.NewResponseController(w)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		rc.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			running := k.isRunning(api.Key(namespace, pod))
			_, err := io.Copy(w, f)
			if err != nil || !running {
				return
			}
		}
	}
}

// safeName reports whether name can be used as one part of a file path.
func safeName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, `/\`)
}
