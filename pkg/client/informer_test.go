package client_test

import (
	"context"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/apiserver"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// waitFor checks cond until it is true, failing the test after 5 seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func cm(namespace, name, mode string) api.ConfigMap {
	return api.ConfigMap{
		ObjectMeta: api.ObjectMeta{Name: name, Namespace: namespace},
		Data:       map[string]string{"mode": mode},
	}
}

func TestInformerFollowsChanges(t *testing.T) {
	st := store.New()
	ts := httptest.NewServer(apiserver.NewHandler(st))
	defer ts.Close()

	// One object exists before the informer starts: it must come from the list.
	st.ConfigMaps.Create(cm("default", "before", "slow"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inf := client.NewInformer[api.ConfigMap](client.New(ts.URL), "configmaps")
	changed := make(chan struct{}, 1)
	inf.Notify(changed)
	go inf.Run(ctx)

	if !inf.WaitForSync(ctx) {
		t.Fatal("never synced")
	}
	if _, ok := inf.Get("default", "before"); !ok {
		t.Fatal("the object that existed before isn't in the copy")
	}
	<-changed // the first list counts as a change

	// The rest arrive through the watch.
	st.ConfigMaps.Create(cm("default", "after", "fast"))
	waitFor(t, "a created object", func() bool { _, ok := inf.Get("default", "after"); return ok })

	before, _ := st.ConfigMaps.Get("default", "before")
	before.Data = map[string]string{"mode": "medium"}
	st.ConfigMaps.Update(before)
	waitFor(t, "an updated object", func() bool { obj, _ := inf.Get("default", "before"); return obj.Data["mode"] == "medium" })

	st.ConfigMaps.Delete("default", "after")
	waitFor(t, "a deleted object", func() bool { _, ok := inf.Get("default", "after"); return !ok })

	select {
	case <-changed:
	default:
		t.Error("Notify's channel got no signal after the changes")
	}
	if n := len(inf.List("")); n != 1 {
		t.Errorf("got %d objects, want 1", n)
	}
}

func TestInformerStartsOverWhenTheServerComesBack(t *testing.T) {
	st := store.New()
	ts := httptest.NewServer(apiserver.NewHandler(st))
	old := client.RetryDelay
	client.RetryDelay = 50 * time.Millisecond
	t.Cleanup(func() { client.RetryDelay = old })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inf := client.NewInformer[api.Namespace](client.New(ts.URL), "namespaces")
	go inf.Run(ctx)
	inf.WaitForSync(ctx)

	// The API server goes away, and its data changes meanwhile.
	addr := ts.Listener.Addr().String()
	ts.CloseClientConnections()
	ts.Close()
	st.CreateNamespace(api.Namespace{ObjectMeta: api.ObjectMeta{Name: "dev"}})

	// It comes back on the same address: the informer lists again.
	ts2 := httptest.NewUnstartedServer(apiserver.NewHandler(st))
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("can't listen on %s again: %v", addr, err)
	}
	ts2.Listener = l
	ts2.Start()

	waitFor(t, "the namespace created while the server was down", func() bool {
		_, ok := inf.Get("", "dev")
		return ok
	})

	// Stop the informer first: Close waits for open connections, such as its watch.
	cancel()
	ts2.Close()
}
