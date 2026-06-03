package main

import "testing"

func Test_format(t *testing.T) {
	// 0|41-bit|10-bit|12-bit
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		timestamp int64
		nodeID    int64
		counter   uint16
		want      int64
	}{
		{
			name:      "success",
			timestamp: 1,
			nodeID:    1,
			counter:   1,
			want:      (1 << 22) | (1 << 12) | 1,
			// 2^22= 4194304
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := format(tt.timestamp, tt.nodeID, tt.counter)
			if tt.want != got {
				t.Errorf("format() = %v, want %v", got, tt.want)
			}
		})
	}
}
