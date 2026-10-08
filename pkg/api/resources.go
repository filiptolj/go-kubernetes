package api

import (
	"fmt"
	"strconv"
	"strings"
)

// The resources a container can ask for.
const (
	ResourceCPU    = "cpu"
	ResourceMemory = "memory"
)

// ResourceRequirements says how much CPU and memory a container needs, and
// how much it may use, such as {"cpu": "250m", "memory": "64Mi"}.
//
// Requests are what the scheduler counts: a pod only goes to a node with
// enough CPU and memory left that isn't requested by other pods. Limits are
// enforced while the container runs: it gets no more CPU time than its
// limit, and is killed (OOMKilled) if it uses more memory than its limit.
type ResourceRequirements struct {
	Requests ResourceList `json:"requests,omitempty"`
	Limits   ResourceList `json:"limits,omitempty"`
}

// ParseCPU reads an amount of CPU and returns it in millicores, thousandths
// of a CPU: "2" is 2000, "0.5" and "500m" are both 500.
func ParseCPU(s string) (int64, error) {
	if milli, ok := strings.CutSuffix(s, "m"); ok {
		n, err := strconv.ParseInt(milli, 10, 64)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid cpu %q: use a number of CPUs (\"0.5\") or millicores (\"500m\")", s)
		}
		return n, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("invalid cpu %q: use a number of CPUs (\"0.5\") or millicores (\"500m\")", s)
	}
	return int64(f*1000 + 0.5), nil
}

// memoryUnits are the suffixes ParseMemory understands. Ki, Mi and Gi are
// powers of 1024; k, M and G are powers of 1000, as in Kubernetes.
var memoryUnits = []struct {
	suffix string
	bytes  int64
}{
	{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40},
	{"k", 1e3}, {"M", 1e6}, {"G", 1e9}, {"T", 1e12},
}

// ParseMemory reads an amount of memory, such as "128Mi" or "1G", and
// returns it in bytes. A plain number is a number of bytes.
func ParseMemory(s string) (int64, error) {
	number, unit := s, int64(1)
	for _, u := range memoryUnits {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			number, unit = n, u.bytes
			break
		}
	}
	n, err := strconv.ParseInt(number, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid memory %q: use a number with a unit, such as \"128Mi\" or \"1Gi\"", s)
	}
	return n * unit, nil
}

// FormatCPU writes millicores the way people write them: "2" or "250m".
func FormatCPU(milli int64) string {
	if milli%1000 == 0 {
		return strconv.FormatInt(milli/1000, 10)
	}
	return strconv.FormatInt(milli, 10) + "m"
}

// FormatMemory writes bytes in the largest unit that keeps them whole, such
// as "64Mi".
func FormatMemory(bytes int64) string {
	for _, u := range []struct {
		suffix string
		bytes  int64
	}{{"Gi", 1 << 30}, {"Mi", 1 << 20}, {"Ki", 1 << 10}} {
		if bytes != 0 && bytes%u.bytes == 0 {
			return strconv.FormatInt(bytes/u.bytes, 10) + u.suffix
		}
	}
	return strconv.FormatInt(bytes, 10)
}

// Resources is an amount of CPU (in millicores) and memory (in bytes).
type Resources struct {
	CPU    int64
	Memory int64
}

// Add returns r plus other.
func (r Resources) Add(other Resources) Resources {
	return Resources{r.CPU + other.CPU, r.Memory + other.Memory}
}

// ParseResources reads a ResourceList that the API server has checked.
// Missing resources count as 0.
func ParseResources(list ResourceList) Resources {
	var r Resources
	if s, ok := list[ResourceCPU]; ok {
		r.CPU, _ = ParseCPU(s)
	}
	if s, ok := list[ResourceMemory]; ok {
		r.Memory, _ = ParseMemory(s)
	}
	return r
}

// Requests returns how much CPU and memory a pod requests: the sum of its
// containers' requests. Init containers run one at a time, before the
// others, so the pod needs at least as much as the largest of them, as in
// Kubernetes.
func (spec PodSpec) Requests() Resources {
	var total Resources
	for _, c := range spec.Containers {
		total = total.Add(ParseResources(c.Resources.Requests))
	}
	for _, c := range spec.InitContainers {
		r := ParseResources(c.Resources.Requests)
		total.CPU = max(total.CPU, r.CPU)
		total.Memory = max(total.Memory, r.Memory)
	}
	return total
}
