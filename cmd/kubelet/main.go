package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/cri"
	"github.com/filiptolj/go-kubernetes/pkg/kubelet"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "API server address")
	nodeName := flag.String("node", "node-1", "name of this node")
	cpu := flag.Int("cpu", 4, "CPU cores this node offers")
	memory := flag.Int("memory", 8192, "memory this node offers, in MiB")
	runtimeName := flag.String("runtime", "docker", "how to run containers: docker or process")
	logDir := flag.String("log-dir", "", "where to write container logs (default <temp dir>/minik8s/<node>)")
	port := flag.Int("port", 10250, "port to serve container logs on (give each kubelet on one machine its own)")
	flag.Parse()

	var rt cri.Runtime
	switch *runtimeName {
	case "docker":
		rt = cri.DockerRuntime{Node: *nodeName}
	case "process":
		rt = cri.NewProcessRuntime()
	default:
		log.Fatalf("unknown runtime %q", *runtimeName)
	}

	if *logDir == "" {
		*logDir = filepath.Join(os.TempDir(), "minik8s", *nodeName)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	k := kubelet.New(kubelet.Config{
		NodeName:    *nodeName,
		CPU:         *cpu,
		Memory:      *memory,
		LogDir:      *logDir,
		Heartbeat:   5 * time.Second,
		ListenAddr:  fmt.Sprintf(":%d", *port),
		GracePeriod: 2 * time.Second,
	}, client.New(*server), rt)

	log.Printf("kubelet for node %q started (runtime: %s, logs in %s)", *nodeName, *runtimeName, *logDir)

	err := k.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("kubelet stopped")
}
