package main

import (
	"slices"
	"testing"
)

func TestSplitNamespaceFlags(t *testing.T) {
	tests := []struct {
		args          []string
		wantRest      []string
		wantNamespace string
		wantGiven     bool
		wantAll       bool
	}{
		{[]string{"get", "pods"}, []string{"get", "pods"}, "default", false, false},
		{[]string{"get", "pods", "-n", "dev"}, []string{"get", "pods"}, "dev", true, false},
		{[]string{"-n", "dev", "get", "pods", "-w"}, []string{"get", "pods", "-w"}, "dev", true, false},
		{[]string{"get", "pods", "--namespace=dev"}, []string{"get", "pods"}, "dev", true, false},
		{[]string{"get", "pods", "-n=dev"}, []string{"get", "pods"}, "dev", true, false},
		{[]string{"get", "pods", "-A"}, []string{"get", "pods"}, "default", false, true},
		{[]string{"logs", "web-x", "-n", "dev", "-f"}, []string{"logs", "web-x", "-f"}, "dev", true, false},
	}

	for _, tt := range tests {
		rest, namespace, given, all, err := splitNamespaceFlags(tt.args)
		if err != nil {
			t.Errorf("%v: unexpected error %v", tt.args, err)
			continue
		}
		if !slices.Equal(rest, tt.wantRest) || namespace != tt.wantNamespace || given != tt.wantGiven || all != tt.wantAll {
			t.Errorf("%v: got %v %q given=%t all=%t, want %v %q given=%t all=%t",
				tt.args, rest, namespace, given, all, tt.wantRest, tt.wantNamespace, tt.wantGiven, tt.wantAll)
		}
	}

	_, _, _, _, err := splitNamespaceFlags([]string{"get", "pods", "-n"})
	if err == nil {
		t.Error("-n without a value: got no error")
	}
}
