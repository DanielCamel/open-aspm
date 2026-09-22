package blobstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestGeneratedKeysRoundTrip(t *testing.T) {
	key, err := NewKey()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseKey(key.String())
	if err != nil {
		t.Fatal(err)
	}
	if parsed != key {
		t.Fatalf("ParseKey() = %q, want %q", parsed.String(), key.String())
	}
}

func TestParseKeyRejectsPaths(t *testing.T) {
	for _, value := range []string{"../secret", "/tmp/blob", `blb_..\\secret`, "client-name"} {
		if _, err := ParseKey(value); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("ParseKey(%q) error = %v, want ErrInvalidKey", value, err)
		}
	}
}

func TestBoundedReaderEnforcesLimit(t *testing.T) {
	reader := NewBoundedReader(context.Background(), strings.NewReader("12345"), 4)
	buffer := make([]byte, 8)
	if _, err := reader.Read(buffer); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Read() error = %v, want ErrTooLarge", err)
	}
}

func TestBoundedReaderEnforcesDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	reader := NewBoundedReader(ctx, strings.NewReader("data"), 10)
	if _, err := reader.Read(make([]byte, 4)); !errors.Is(err, ErrDeadline) {
		t.Fatalf("Read() error = %v, want ErrDeadline", err)
	}
}
