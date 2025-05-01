package main

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"testing"
	"strconv"
)

func randomUint32() (uint32, error) {
	var num uint32
	err := binary.Read(rand.Reader, binary.LittleEndian, &num)
	if err != nil {
		return 0, err
	}
	return num, nil
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected uint64
		hasError bool
	}{
		{"Empty input", "", 0, true},
		{"Valid KB (lowercase)", "1024kb", 1024 * 1024, false},
		{"Valid KB (uppercase)", "1024KB", 1024 * 1024, false},
		{"Valid MB", "1MB", 1 * 1024 * 1024, false},
		{"Valid GB", "1GB", 1 * 1024 * 1024 * 1024, false},
		{"Unsupported TB", "1TB", 0, true},
		{"Overflow, KB", strconv.FormatUint(math.MaxUint64, 10) + "KB", 0, true},
	}

	for _, testcase := range tests {
		t.Run(testcase.name, func(t *testing.T) {
			size, err := parseSize(testcase.input)
			if (err != nil) != testcase.hasError {
				t.Errorf("Expected error: %v, got: %v", testcase.hasError, err)
			}
			if size != testcase.expected {
				t.Errorf("Expected size: %d, got: %d", testcase.expected, size)
			}
		})
	}
}
