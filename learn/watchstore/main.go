package main

import (
	"fmt"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

func main() {
	s := store.New()

	events, stop := s.Watch("pods", "")
	defer stop()

	go func() {
		for _, name := range []string{"nginx", "redis", "postgres"} {
			time.Sleep(time.Second)
			fmt.Println("creating", name)
			s.CreatePod(api.Pod{
				ObjectMeta: api.ObjectMeta{Name: name, Namespace: api.DefaultNamespace},
				PodStatus:  api.PodStatus{Phase: api.PodPending},
			})
		}
	}()

	for i := 0; i < 3; i++ {
		event := <-events
		fmt.Printf("watcher got: %s %s\n", event.Type, event.Object.(api.Pod).Name)
	}
	fmt.Println("done")
}
