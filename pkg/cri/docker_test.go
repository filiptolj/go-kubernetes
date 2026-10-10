package cri

import "testing"

func TestParseDockerSize(t *testing.T) {
	for s, want := range map[string]int64{"3.07MiB": 3219128, "512kB": 512000, "1GiB": 1 << 30, "0B": 0} {
		got, err := parseDockerSize(s)
		if err != nil || got != want {
			t.Errorf("parseDockerSize(%q) = %d, %v; want %d", s, got, err, want)
		}
	}
	if _, err := parseDockerSize("lots"); err == nil {
		t.Error("parseDockerSize(lots): want an error")
	}
}
