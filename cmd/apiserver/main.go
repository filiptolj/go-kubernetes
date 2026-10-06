package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/filiptolj/go-kubernetes/pkg/apiserver"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dataFile := flag.String("data", "data/apiserver.json", "file to save the cluster state in (empty: keep it in memory only)")
	etcd := flag.String("etcd", "", "save the cluster state in etcd at these comma-separated endpoints, such as localhost:2379, instead of a file")
	flag.Parse()

	st, err := openStore(*etcd, *dataFile)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	log.Printf("mini-apiserver listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, apiserver.NewHandler(st)))
}

// openStore picks where the cluster state is kept: etcd if endpoints are
// given, otherwise the data file, otherwise memory only.
func openStore(etcd, dataFile string) (*store.Store, error) {
	switch {
	case etcd != "":
		backend, err := store.OpenEtcd(strings.Split(etcd, ","))
		if err != nil {
			return nil, err
		}
		log.Printf("saving cluster state in etcd at %s", etcd)
		return store.Open(backend)

	case dataFile != "":
		backend, err := store.OpenFile(dataFile)
		if err != nil {
			return nil, err
		}
		log.Printf("saving cluster state in %s", dataFile)
		return store.Open(backend)

	default:
		log.Printf("keeping cluster state in memory only")
		return store.New(), nil
	}
}
