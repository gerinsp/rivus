package util

import "testing"

func TestNormalizeMySQLPartialDate(t *testing.T) {
	tests := []struct {
		input      string
		want       string
		wantChange bool
	}{
		{input: "2014-05-00", want: "2014-05-01", wantChange: true},
		{input: "2014-00-00 12:30:00", want: "2014-01-01 12:30:00", wantChange: true},
		{input: "0000-00-00", want: "1970-01-01", wantChange: true},
		{input: "2014-05-20", want: "2014-05-20", wantChange: false},
		{input: "ordinary text", want: "ordinary text", wantChange: false},
	}
	for _, test := range tests {
		got, changed := NormalizeMySQLPartialDate(test.input)
		if got != test.want || changed != test.wantChange {
			t.Errorf("NormalizeMySQLPartialDate(%q) = %q/%t, want %q/%t", test.input, got, changed, test.want, test.wantChange)
		}
	}
}
