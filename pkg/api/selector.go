package api

import (
	"fmt"
	"slices"
	"strings"
)

// Selector picks objects by their labels, like `minikubectl get pods -l
// app=web`. It is a list of requirements that must all hold. It understands
// the same forms as Kubernetes:
//
//	app=web  app==web    the label app is web
//	app!=web             app isn't web (or there is no app label)
//	env in (prod,dev)    env is one of these
//	env notin (test)     env is none of these (or there is no env label)
//	app                  there is an app label
//	!app                 there is no app label
//
// The empty Selector matches every object.
type Selector []requirement

type requirement struct {
	key    string
	op     string // "in", "notin", "exists" or "!exists"
	values []string
}

// ParseSelector reads a selector, such as "app=web,tier!=db".
func ParseSelector(s string) (Selector, error) {
	var sel Selector
	for _, part := range splitSelector(s) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		r, err := parseRequirement(part)
		if err != nil {
			return nil, fmt.Errorf("invalid selector %q: %w", s, err)
		}
		sel = append(sel, r)
	}
	return sel, nil
}

// splitSelector splits a selector at its commas, except the commas inside
// the parentheses of "in (a,b)".
func splitSelector(s string) []string {
	var parts []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}

func parseRequirement(s string) (requirement, error) {
	// key!=value, key==value, key=value
	for _, op := range []string{"!=", "==", "="} {
		if key, value, ok := strings.Cut(s, op); ok {
			key, value = strings.TrimSpace(key), strings.TrimSpace(value)
			if key == "" {
				return requirement{}, fmt.Errorf("%q has no label name", s)
			}
			if op == "!=" {
				return requirement{key, "notin", []string{value}}, nil
			}
			return requirement{key, "in", []string{value}}, nil
		}
	}

	// key in (a,b), key notin (a,b)
	fields := strings.Fields(s)
	if len(fields) >= 2 && (fields[1] == "in" || strings.HasPrefix(fields[1], "in(") ||
		fields[1] == "notin" || strings.HasPrefix(fields[1], "notin(")) {
		key := fields[0]
		rest := strings.TrimSpace(strings.TrimPrefix(s, key))
		op := "in"
		if strings.HasPrefix(rest, "notin") {
			op = "notin"
		}
		list := strings.TrimSpace(strings.TrimPrefix(rest, op))
		if !strings.HasPrefix(list, "(") || !strings.HasSuffix(list, ")") {
			return requirement{}, fmt.Errorf("%q: put the values in parentheses, like %s %s (a,b)", s, key, op)
		}
		var values []string
		for _, v := range strings.Split(list[1:len(list)-1], ",") {
			values = append(values, strings.TrimSpace(v))
		}
		return requirement{key, op, values}, nil
	}

	// !key, key
	if key, ok := strings.CutPrefix(s, "!"); ok {
		return requirement{strings.TrimSpace(key), "!exists", nil}, nil
	}
	if strings.ContainsAny(s, " ()") {
		return requirement{}, fmt.Errorf("can't read %q", s)
	}
	return requirement{s, "exists", nil}, nil
}

// Matches reports whether labels meet every requirement of the selector.
func (sel Selector) Matches(labels Labels) bool {
	for _, r := range sel {
		value, ok := labels[r.key]
		var holds bool
		switch r.op {
		case "in":
			holds = ok && slices.Contains(r.values, value)
		case "notin":
			holds = !ok || !slices.Contains(r.values, value)
		case "exists":
			holds = ok
		case "!exists":
			holds = !ok
		}
		if !holds {
			return false
		}
	}
	return true
}
