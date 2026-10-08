package cron

import (
	"testing"
	"time"
)

// at makes a time in UTC, to keep the tests independent of the machine's time zone.
func at(year int, month time.Month, day, hour, minute int) time.Time {
	return time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
}

func TestNext(t *testing.T) {
	// 2026-10-08 was a Thursday.
	from := at(2026, 10, 8, 10, 7)

	tests := []struct {
		spec string
		want time.Time
	}{
		{"* * * * *", at(2026, 10, 8, 10, 8)},
		{"*/15 * * * *", at(2026, 10, 8, 10, 15)},
		{"0 * * * *", at(2026, 10, 8, 11, 0)},
		{"@hourly", at(2026, 10, 8, 11, 0)},
		{"30 9 * * *", at(2026, 10, 9, 9, 30)},       // 9:30 today is past, so tomorrow
		{"0 0 1 * *", at(2026, 11, 1, 0, 0)},         // first of the month
		{"0 12 * * 1-5", at(2026, 10, 8, 12, 0)},     // weekdays at noon: today
		{"0 12 * * 0", at(2026, 10, 11, 12, 0)},      // Sundays
		{"0 12 * * 7", at(2026, 10, 11, 12, 0)},      // 7 is Sunday too
		{"0 0 1 * 1", at(2026, 10, 12, 0, 0)},        // the 1st OR a Monday: Monday comes first
		{"0 0 29 2 *", at(2028, 2, 29, 0, 0)},        // February 29th: the next leap year
		{"5,10 8-9 * * *", at(2026, 10, 9, 8, 5)},    // a list and a range
		{"0-30/10 * * * *", at(2026, 10, 8, 10, 10)}, // a range with a step
	}

	for _, tt := range tests {
		s, err := Parse(tt.spec)
		if err != nil {
			t.Errorf("Parse(%q): %v", tt.spec, err)
			continue
		}
		if got := s.Next(from); !got.Equal(tt.want) {
			t.Errorf("%q: next after %s is %s, want %s", tt.spec, from.Format(time.DateTime), got.Format(time.DateTime), tt.want.Format(time.DateTime))
		}
	}
}

func TestNeverDue(t *testing.T) {
	s, err := Parse("0 0 31 2 *") // February 31st
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Next(at(2026, 1, 1, 0, 0)); !got.IsZero() {
		t.Errorf("got %s, want the zero time", got)
	}
}

func TestParseErrors(t *testing.T) {
	for _, spec := range []string{
		"",
		"* * * *",     // 4 fields
		"60 * * * *",  // minute too big
		"* 24 * * *",  // hour too big
		"* * 0 * *",   // day 0
		"* * * 13 *",  // month 13
		"*/0 * * * *", // step 0
		"a * * * *",
		"5-1 * * * *", // backwards range
	} {
		_, err := Parse(spec)
		if err == nil {
			t.Errorf("Parse(%q): got no error", spec)
		}
	}
}
