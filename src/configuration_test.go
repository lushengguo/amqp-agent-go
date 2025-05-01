package main

import (
	"crypto/rand"
	"encoding/binary"
	"strconv"
	"testing"
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
	{
		_, err := parseSize("")
		if err == nil {
			t.Errorf("Expected no error, got %v", err)
		}
	}

	// little case suffix is also supported
	{
		base, err := randomUint32()
		if err != nil {
			t.Errorf("randomUint32 is not expected to return error")
		}
		size, err := parseSize(strconv.Itoa(int(base)) + "kb")
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if size != uint64(base)*1024 {
			t.Errorf("Expected size 10KB, got %d", size)
		}
	}

	{
		base, err := randomUint32()
		if err != nil {
			t.Errorf("randomUint32 is not expected to return error")
		}
		size, err := parseSize(strconv.Itoa(int(base)) + "KB")
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if size != uint64(base)*1024 {
			t.Errorf("Expected size 10KB, got %d", size)
		}
	}

	{
		base, err := randomUint32()
		if err != nil {
			t.Errorf("randomUint32 is not expected to return error")
		}
		size, err := parseSize(strconv.Itoa(int(base)) + "MB")
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if size != uint64(base)*1024*1024 {
			t.Errorf("Expected size 10KB, got %d", size)
		}
	}

	{
		base, err := randomUint32()
		if err != nil {
			t.Errorf("randomUint32 is not expected to return error")
		}
		size, err := parseSize(strconv.Itoa(int(base)) + "GB")
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if size != uint64(base)*1024*1024*1024 {
			t.Errorf("Expected size 10KB, got %d", size)
		}
	}

	{
		_, err := parseSize("1TB")
		if err == nil {
			t.Errorf("TB suffix is not supported: %v", err)
		}
	}
}
