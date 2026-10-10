package client

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// GetPodMetrics asks the kubelet at address, such as
// "http://localhost:10250", what its pods use. The autoscaler and
// `minikubectl top` use it.
func GetPodMetrics(address string) ([]api.PodMetrics, error) {
	c := http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(address + "/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kubelet at %s: %w", address, statusError(resp))
	}

	var metrics []api.PodMetrics
	err = json.NewDecoder(resp.Body).Decode(&metrics)
	if err != nil {
		return nil, fmt.Errorf("kubelet at %s: %w", address, err)
	}
	return metrics, nil
}
