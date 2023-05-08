package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSizeConverter(t *testing.T) {
	require := require.New(t)
	tests := map[string]struct {
		size string
		want float64
	}{
		"terabytesConversion": {
			size: "1 TiB",
			want: 1099511627776.0,
		},
		"gigabytesConversion": {
			size: "1 GiB",
			want: 1073741824.0,
		},
		"megabytesConversion": {
			size: "1 MiB",
			want: 1048576.0,
		},
		"kilobytesConversion": {
			size: "1 KiB",
			want: 1024.0,
		},
		"bytesConversion": {
			size: "1 B",
			want: 1.0,
		},
		"emptyConversion": {
			size: " ",
			want: 0.0,
		},
	}
	for name, tt := range tests {
		testCase := tt
		t.Run(name, func(t *testing.T) {
			require.EqualValues(testCase.want, GetSizeFromStringtoFloat(testCase.size))
		})
	}
}
