package utils

import (
	"testing"
)

func TestSizeConverter(t *testing.T) {
	size := "1 TiB"
	want := 1.0 * 1099511627776
	got := GetSizeFromStringtoFloat(size)
	if got != want {
		t.Errorf("Expected size: %v, got: %v", want, got)
	}

	size = "1 GiB"
	want = 1.0 * 1073741824
	got = GetSizeFromStringtoFloat(size)
	if got != want {
		t.Errorf("Expected size: %v, got: %v", want, got)
	}

	size = "1 MiB"
	want = 1.0 * 1048576
	got = GetSizeFromStringtoFloat(size)
	if got != want {
		t.Errorf("Expected size: %v, got: %v", want, got)
	}

	size = "1 KiB"
	want = 1.0 * 1024
	got = GetSizeFromStringtoFloat(size)
	if got != want {
		t.Errorf("Expected size: %v, got: %v", want, got)
	}

	size = "1 B"
	want = 1.0
	got = GetSizeFromStringtoFloat(size)
	if got != want {
		t.Errorf("Expected size: %v, got: %v", want, got)
	}

	size = " "
	want = 0.0
	got = GetSizeFromStringtoFloat(size)
	if got != want {
		t.Errorf("Expected size: %v, got: %v", want, got)
	}
}