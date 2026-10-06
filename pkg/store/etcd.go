package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdBackend keeps objects in etcd, the database real Kubernetes uses. Each
// object gets its own key, "/minik8s/<kind>/<name>", with its JSON as the
// value. You can look at them with etcdctl:
//
//	etcdctl get --prefix /minik8s/ --keys-only
type EtcdBackend struct {
	client *clientv3.Client
}

// etcdPrefix comes before every key we write, so our keys can't clash with
// anything else stored in the same etcd.
const etcdPrefix = "/minik8s/"

// etcdTimeout is how long we wait for etcd before giving up on a request.
// It is shorter than the clients' own 5-second timeout, so when etcd is down
// they get a clear error from the API server instead of timing out first.
const etcdTimeout = 3 * time.Second

// OpenEtcd connects to etcd at the given endpoints, such as
// "localhost:2379".
func OpenEtcd(endpoints []string) (*EtcdBackend, error) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return nil, err
	}

	// New doesn't actually connect. Ask etcd for its status, so a wrong
	// address fails now, with a clear message, instead of on the first write.
	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	defer cancel()

	_, err = c.Status(ctx, endpoints[0])
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("connect to etcd at %s: %w", strings.Join(endpoints, ","), err)
	}
	return &EtcdBackend{client: c}, nil
}

// key returns the etcd key for an object.
func key(kind, name string) string {
	return etcdPrefix + kind + "/" + name
}

// LoadAll reads every key under our prefix.
func (e *EtcdBackend) LoadAll() (map[string]map[string][]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	defer cancel()

	resp, err := e.client.Get(ctx, etcdPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	all := make(map[string]map[string][]byte)
	for _, kv := range resp.Kvs {
		// "/minik8s/pods/nginx" -> kind "pods", name "nginx"
		kind, name, ok := strings.Cut(strings.TrimPrefix(string(kv.Key), etcdPrefix), "/")
		if !ok {
			continue
		}
		if all[kind] == nil {
			all[kind] = make(map[string][]byte)
		}
		all[kind][name] = kv.Value
	}
	return all, nil
}

// Put saves one object.
func (e *EtcdBackend) Put(kind, name string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	defer cancel()

	_, err := e.client.Put(ctx, key(kind, name), string(data))
	return err
}

// Delete removes one object.
func (e *EtcdBackend) Delete(kind, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	defer cancel()

	_, err := e.client.Delete(ctx, key(kind, name))
	return err
}

// Close disconnects from etcd.
func (e *EtcdBackend) Close() error {
	return e.client.Close()
}
