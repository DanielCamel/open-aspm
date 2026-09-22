// Package blobstoretest provides one conformance suite for every adapter.
package blobstoretest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/spectremi/open-aspm/internal/blobstore"
)

// Factory returns an isolated store. Cleanup is handled through Store.Close.
type Factory func(*testing.T) blobstore.Store

// Run executes the backend-neutral BlobStore contract.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("stream round trip", func(t *testing.T) {
		store := openStore(t, factory)
		content := bytes.Repeat([]byte("x"), 20<<20)
		key := newKey(t)
		expectedSize := int64(len(content))
		expectedDigest := digest(content)

		metadata, err := store.Put(context.Background(), key, bytes.NewReader(content), blobstore.PutOptions{
			MaxBytes:       expectedSize + 1,
			Timeout:        10 * time.Second,
			ExpectedSize:   &expectedSize,
			ExpectedSHA256: expectedDigest,
		})
		if err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		if metadata.Size != expectedSize || metadata.SHA256 != expectedDigest || metadata.Version == "" {
			t.Fatalf("Put() metadata = %+v", metadata)
		}

		stat, err := store.Stat(context.Background(), key)
		if err != nil {
			t.Fatalf("Stat() error = %v", err)
		}
		if stat != metadata {
			t.Fatalf("Stat() = %+v, want %+v", stat, metadata)
		}

		reader, opened, err := store.Open(context.Background(), key)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		actual, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read errors = %v, %v", readErr, closeErr)
		}
		if !bytes.Equal(actual, content) || opened != metadata {
			t.Fatal("Open() did not return the committed bytes and metadata")
		}

		if _, err := store.Put(context.Background(), key, bytes.NewReader(content), defaultOptions(expectedSize+1)); !errors.Is(err, blobstore.ErrAlreadyExists) {
			t.Fatalf("duplicate Put() error = %v, want ErrAlreadyExists", err)
		}
		if err := store.Delete(context.Background(), key, "wrong-version"); !errors.Is(err, blobstore.ErrVersion) {
			t.Fatalf("Delete(wrong version) error = %v, want ErrVersion", err)
		}
		if err := store.Delete(context.Background(), key, metadata.Version); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if err := store.Delete(context.Background(), key, metadata.Version); err != nil {
			t.Fatalf("idempotent Delete() error = %v", err)
		}
		if _, err := store.Stat(context.Background(), key); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("Stat(deleted) error = %v, want ErrNotFound", err)
		}
	})

	t.Run("byte limit", func(t *testing.T) {
		store := openStore(t, factory)
		key := newKey(t)
		_, err := store.Put(
			context.Background(),
			key,
			bytes.NewReader([]byte("12345")),
			defaultOptions(4),
		)
		if !errors.Is(err, blobstore.ErrTooLarge) {
			t.Fatalf("Put() error = %v, want ErrTooLarge", err)
		}
		if _, err := store.Stat(context.Background(), key); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("oversized object became visible: %v", err)
		}
	})

	t.Run("expected digest", func(t *testing.T) {
		store := openStore(t, factory)
		key := newKey(t)
		options := defaultOptions(64)
		options.ExpectedSHA256 = digest([]byte("different"))
		_, err := store.Put(context.Background(), key, bytes.NewReader([]byte("content")), options)
		if !errors.Is(err, blobstore.ErrIntegrity) {
			t.Fatalf("Put() error = %v, want ErrIntegrity", err)
		}
		if _, err := store.Stat(context.Background(), key); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("hash-mismatched object became visible: %v", err)
		}
	})

	t.Run("time limit", func(t *testing.T) {
		store := openStore(t, factory)
		key := newKey(t)
		_, err := store.Put(context.Background(), key, &slowReader{
			delay: 50 * time.Millisecond,
			data:  []byte("content"),
		}, blobstore.PutOptions{
			MaxBytes: 64,
			Timeout:  10 * time.Millisecond,
		})
		if !errors.Is(err, blobstore.ErrDeadline) {
			t.Fatalf("Put() error = %v, want ErrDeadline", err)
		}
	})
}

func openStore(t *testing.T, factory Factory) blobstore.Store {
	t.Helper()
	store := factory(t)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return store
}

func newKey(t *testing.T) blobstore.Key {
	t.Helper()
	key, err := blobstore.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func defaultOptions(maxBytes int64) blobstore.PutOptions {
	return blobstore.PutOptions{MaxBytes: maxBytes, Timeout: 10 * time.Second}
}

func digest(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

type slowReader struct {
	delay time.Duration
	data  []byte
	read  bool
}

func (reader *slowReader) Read(buffer []byte) (int, error) {
	if reader.read {
		return 0, io.EOF
	}
	time.Sleep(reader.delay)
	reader.read = true
	return copy(buffer, reader.data), nil
}
