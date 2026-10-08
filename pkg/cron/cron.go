// Package cron reads cron schedules, such as "*/5 * * * *", and works out
// when they are next due.
//
// A schedule has five fields, separated by spaces:
//
//	minute (0-59)  hour (0-23)  day of month (1-31)  month (1-12)  day of week (0-6, Sunday is 0 or 7)
//
// Each field is a comma-separated list of: "*" (every value), a number
// ("5"), a range ("1-5"), and optionally a step ("*/15", "0-30/10"). Instead
// of five fields, a schedule can be "@hourly", "@daily" or "@weekly".
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed cron schedule. Each field is a set of allowed values,
// kept as the bits of a number: bit n is 1 when value n is allowed. For
// example, minutes 0, 15, 30 and 45 are the bits 1<<0 | 1<<15 | 1<<30 | 1<<45.
type Schedule struct {
	minute, hour, dom, month, dow uint64

	// In cron, if both day fields are restricted, a day matches when EITHER
	// one does: "1 * 1 * 1" runs on the 1st of the month and on Mondays.
	domStar, dowStar bool
}

// field describes the values one field allows.
type field struct {
	name     string
	min, max int
}

var fields = []field{
	{"minute", 0, 59},
	{"hour", 0, 23},
	{"day of month", 1, 31},
	{"month", 1, 12},
	{"day of week", 0, 7}, // 7 is Sunday too
}

// Parse reads a schedule.
func Parse(spec string) (Schedule, error) {
	switch strings.TrimSpace(spec) {
	case "@hourly":
		spec = "0 * * * *"
	case "@daily", "@midnight":
		spec = "0 0 * * *"
	case "@weekly":
		spec = "0 0 * * 0"
	}

	parts := strings.Fields(spec)
	if len(parts) != 5 {
		return Schedule{}, fmt.Errorf("schedule %q needs 5 fields (minute hour day-of-month month day-of-week), not %d", spec, len(parts))
	}

	var bits [5]uint64
	for i, part := range parts {
		b, err := parseField(part, fields[i])
		if err != nil {
			return Schedule{}, fmt.Errorf("schedule %q: %w", spec, err)
		}
		bits[i] = b
	}

	// Sunday can be written as 0 or 7: move bit 7 to bit 0.
	if bits[4]&(1<<7) != 0 {
		bits[4] = bits[4]&^(1<<7) | 1
	}

	return Schedule{
		minute: bits[0], hour: bits[1], dom: bits[2], month: bits[3], dow: bits[4],
		domStar: parts[2] == "*", dowStar: parts[4] == "*",
	}, nil
}

// parseField reads one field, such as "*/15" or "1-5,10", into a set of bits.
func parseField(text string, f field) (uint64, error) {
	var bits uint64
	for _, item := range strings.Split(text, ",") {
		// Split off a step: "0-30/10" -> range "0-30", step 10.
		rng, stepText, hasStep := strings.Cut(item, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 {
				return 0, fmt.Errorf("%s: bad step %q", f.name, stepText)
			}
			step = n
		}

		// Work out the range: "*", "5" or "1-5".
		lo, hi := f.min, f.max
		if rng != "*" {
			loText, hiText, isRange := strings.Cut(rng, "-")
			var err error
			lo, err = strconv.Atoi(loText)
			if err != nil {
				return 0, fmt.Errorf("%s: %q is not a number", f.name, loText)
			}
			hi = lo
			if isRange {
				hi, err = strconv.Atoi(hiText)
				if err != nil {
					return 0, fmt.Errorf("%s: %q is not a number", f.name, hiText)
				}
			} else if hasStep {
				hi = f.max // "5/10" means "from 5 to the end, every 10"
			}
		}
		if lo < f.min || hi > f.max || lo > hi {
			return 0, fmt.Errorf("%s: %q is outside %d-%d", f.name, rng, f.min, f.max)
		}

		for v := lo; v <= hi; v += step {
			bits |= 1 << v
		}
	}
	return bits, nil
}

// has reports whether bit v is set in bits.
func has(bits uint64, v int) bool {
	return bits&(1<<v) != 0
}

// Next returns the first time after t that the schedule is due, in t's time
// zone. Times are whole minutes: seconds are ignored.
func (s Schedule) Next(t time.Time) time.Time {
	t = t.Truncate(time.Minute).Add(time.Minute)

	// Check minute by minute, but skip whole days and hours that can't
	// match, so even a yearly schedule is found quickly. Give up after five
	// years: a schedule like "0 0 31 2 *" (February 31st) never comes.
	limit := t.AddDate(5, 0, 0)
	for t.Before(limit) {
		if !has(s.month, int(t.Month())) || !s.dayMatches(t) {
			// Jump to midnight of the next day.
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !has(s.hour, t.Hour()) {
			t = t.Truncate(time.Hour).Add(time.Hour)
			continue
		}
		if !has(s.minute, t.Minute()) {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}
	return time.Time{}
}

// dayMatches applies cron's rule for the two day fields.
func (s Schedule) dayMatches(t time.Time) bool {
	dom := has(s.dom, t.Day())
	dow := has(s.dow, int(t.Weekday()))

	switch {
	case s.domStar && s.dowStar:
		return true
	case s.domStar:
		return dow
	case s.dowStar:
		return dom
	default:
		return dom || dow
	}
}
