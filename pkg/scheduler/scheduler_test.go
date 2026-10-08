package scheduler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/apiserver"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// TestBurstOfPodsIsSpreadOut creates 6 pods at once while the scheduler's
// informers lag behind. The scheduler must remember where it put each pod,
// or it would see the same empty nodes for all of them.
func TestBurstOfPodsIsSpreadOut(t *testing.T) {
	st := store.New()
	ts := httptest.NewServer(slowWatches(apiserver.NewHandler(st), 50*time.Millisecond))
	t.Cleanup(ts.Close)
	c := client.New(ts.URL)

	st.PutNode(node("node-1", true))
	st.PutNode(node("node-2", true))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // before ts.Close, which waits for the watches to end

	s := &Scheduler{
		Client: c,
		Picker: &LeastLoaded{},
		Pods:   client.NewInformer[api.Pod](c, "pods"),
		Nodes:  client.NewInformer[api.Node](c, "nodes"),
		Resync: time.Hour,
	}
	go s.Pods.Run(ctx)
	go s.Nodes.Run(ctx)
	go s.Run(ctx)
	s.Pods.WaitForSync(ctx)

	for i := range 6 {
		st.CreatePod(api.Pod{
			ObjectMeta: api.ObjectMeta{Name: fmt.Sprintf("pod-%d", i), Namespace: api.DefaultNamespace},
			PodStatus:  api.PodStatus{Phase: api.PodPending},
		})
	}

	perNode := map[string]int{}
	deadline := time.Now().Add(5 * time.Second)
	for {
		clear(perNode)
		for _, pod := range st.ListPods("") {
			perNode[pod.NodeName]++
		}
		if perNode[""] == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if perNode["node-1"] != 3 || perNode["node-2"] != 3 {
		t.Errorf("got pods per node %v, want 3 on each", perNode)
	}
}

// slowWatches wraps an API server so that watches deliver each event late.
func slowWatches(h http.Handler, delay time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") == "true" {
			w = &slowWriter{ResponseWriter: w, delay: delay}
		}
		h.ServeHTTP(w, r)
	})
}

type slowWriter struct {
	http.ResponseWriter
	delay time.Duration
}

func (w *slowWriter) Write(p []byte) (int, error) {
	time.Sleep(w.delay)
	return w.ResponseWriter.Write(p)
}

// Unwrap lets http.NewResponseController reach the real writer, to flush it.
func (w *slowWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
