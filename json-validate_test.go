package pj

import (
	"testing"
)

func TestCheckValidJSON(t *testing.T) {

	tests := []struct {
		input    string
		expected bool
	}{
		{`{}`, true},
		{`[]`, true},
		{`[""]`, true},
		{`{`, false},
		{`]`, false},
		{`["]`, false},
	}

	for _, test := range tests {
		if got, want := isValidJSON([]byte(test.input)) == nil, test.expected; got != want {
			t.Errorf("isValidJSON(%#v) == nil = %v; want %v", test.input, got, want)
		}
	}

}
