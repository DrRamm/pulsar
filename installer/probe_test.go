package main

import "testing"

func TestArchOfUsesAvailableUserlandABI(t *testing.T) {
	tests := []struct {
		name, code string
		aarch64    bool
		armhf      bool
		want       string
	}{
		{"cucumber prefers aarch64", "cucumber", true, true, "aarch64"},
		{"cucumber falls back to armhf userspace", "cucumber", false, true, "armhf"},
		{"plum prefers armhf", "plum", true, true, "armhf"},
		{"plum without its loader is skipped", "plum", true, false, ""},
		{"no compatible loader", "cucumber", false, false, ""},
		{"unsupported model", "unknown", true, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := archOf(tt.code, tt.aarch64, tt.armhf); got != tt.want {
				t.Fatalf("archOf(%q, %t, %t) = %q, want %q", tt.code, tt.aarch64, tt.armhf, got, tt.want)
			}
		})
	}
}
