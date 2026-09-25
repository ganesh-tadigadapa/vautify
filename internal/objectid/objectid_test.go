package objectid

import (
	"errors"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	valid := []string{"a", "dataset.bin", "Report_2026-09-26.csv", "x1", strings.Repeat("a", MaxLen)}
	for _, id := range valid {
		if err := Validate(id); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", id, err)
		}
	}

	invalid := []string{
		"", ".", "..", "../etc/passwd", "a/b", `a\b`, ".hidden", "-flag", "_x",
		"a b", "a\x00b", "héllo", strings.Repeat("a", MaxLen+1), "a%2Fb",
	}
	for _, id := range invalid {
		err := Validate(id)
		if err == nil {
			t.Errorf("Validate(%q) = nil, want error", id)
			continue
		}
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("Validate(%q) error %v does not wrap ErrInvalid", id, err)
		}
	}
}
