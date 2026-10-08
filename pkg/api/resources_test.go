package api

import "testing"

func TestParseCPU(t *testing.T) {
	for s, want := range map[string]int64{"2": 2000, "0.5": 500, "500m": 500, "1.25": 1250, "0": 0, "100m": 100} {
		got, err := ParseCPU(s)
		if err != nil || got != want {
			t.Errorf("ParseCPU(%q) = %d, %v; want %d", s, got, err, want)
		}
	}
	for _, s := range []string{"", "lots", "-1", "5x", "m"} {
		if _, err := ParseCPU(s); err == nil {
			t.Errorf("ParseCPU(%q): want an error", s)
		}
	}
}

func TestParseMemory(t *testing.T) {
	for s, want := range map[string]int64{"128Mi": 128 << 20, "1Gi": 1 << 30, "512Ki": 512 << 10, "1G": 1e9, "1500": 1500, "2M": 2e6} {
		got, err := ParseMemory(s)
		if err != nil || got != want {
			t.Errorf("ParseMemory(%q) = %d, %v; want %d", s, got, err, want)
		}
	}
	for _, s := range []string{"", "1.5Gi", "Mi", "-1Mi", "12Q"} {
		if _, err := ParseMemory(s); err == nil {
			t.Errorf("ParseMemory(%q): want an error", s)
		}
	}
}

func TestFormat(t *testing.T) {
	if s := FormatCPU(2000); s != "2" {
		t.Errorf("FormatCPU(2000) = %q, want 2", s)
	}
	if s := FormatCPU(250); s != "250m" {
		t.Errorf("FormatCPU(250) = %q, want 250m", s)
	}
	if s := FormatMemory(64 << 20); s != "64Mi" {
		t.Errorf("FormatMemory(64Mi) = %q, want 64Mi", s)
	}
	if s := FormatMemory(1 << 30); s != "1Gi" {
		t.Errorf("FormatMemory(1Gi) = %q, want 1Gi", s)
	}
}

func TestPodRequests(t *testing.T) {
	spec := PodSpec{Containers: []Container{
		{Resources: ResourceRequirements{Requests: ResourceList{"cpu": "250m", "memory": "64Mi"}}},
		{Resources: ResourceRequirements{Requests: ResourceList{"cpu": "0.5"}}},
		{},
	}}
	got := spec.Requests()
	if got.CPU != 750 || got.Memory != 64<<20 {
		t.Errorf("got %+v, want 750 millicores and 64Mi", got)
	}
}
