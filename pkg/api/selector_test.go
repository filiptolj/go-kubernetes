package api

import "testing"

func TestSelector(t *testing.T) {
	web := Labels{"app": "web", "env": "prod"}
	tests := []struct {
		selector string
		want     bool
	}{
		{"", true},
		{"app=web", true},
		{"app==web", true},
		{"app=db", false},
		{"app!=db", true},
		{"tier!=db", true}, // no tier label: not db
		{"app=web,env=prod", true},
		{"app=web,env=test", false},
		{"env in (prod,dev)", true},
		{"env in (test)", false},
		{"env notin (test,dev)", true},
		{"env in (dev,prod),app", true},
		{"app", true},
		{"tier", false},
		{"!tier", true},
		{"!app", false},
	}
	for _, tt := range tests {
		sel, err := ParseSelector(tt.selector)
		if err != nil {
			t.Errorf("ParseSelector(%q): %v", tt.selector, err)
			continue
		}
		if got := sel.Matches(web); got != tt.want {
			t.Errorf("%q matches %v: got %t, want %t", tt.selector, web, got, tt.want)
		}
	}

	for _, bad := range []string{"=web", "env in prod", "a b"} {
		if _, err := ParseSelector(bad); err == nil {
			t.Errorf("ParseSelector(%q): want an error", bad)
		}
	}
}
