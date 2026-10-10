package cri

import (
	"fmt"
	"os/exec"
	"strings"
)

// The cluster network. Every pod run by the Docker runtime gets an address
// on this Docker network, and so does the cluster's DNS server and Service
// proxy. Pods get addresses from PodRange; the fixed addresses below it are
// for the cluster's own services.
const (
	NetworkName   = "minik8s"
	NetworkSubnet = "10.244.0.0/16"
	PodRange      = "10.244.128.0/17"

	// ClusterProxyIP is where pods find the cluster DNS server and every
	// Service: <service>.<namespace>.svc.cluster.local resolves to it.
	ClusterProxyIP = "10.244.0.2"

	// ClusterDomain is the domain of the cluster's DNS names.
	ClusterDomain = "cluster.local"
)

// EnsureNetwork creates the cluster network if it doesn't exist yet.
// Several kubelets may try at once; whoever loses finds it already there.
func EnsureNetwork() error {
	if exec.Command("docker", "network", "inspect", NetworkName).Run() == nil {
		return nil
	}
	out, err := exec.Command("docker", "network", "create",
		"--subnet", NetworkSubnet, "--ip-range", PodRange,
		"--label", "minik8s=true", NetworkName).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "already exists") {
		return fmt.Errorf("create network %s: %v: %s", NetworkName, err, out)
	}
	return nil
}
