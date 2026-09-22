package filesystem

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spectremi/open-aspm/internal/blobstore"
	"github.com/spectremi/open-aspm/internal/blobstore/blobstoretest"
)

func TestConformance(t *testing.T) {
	blobstoretest.Run(t, func(t *testing.T) blobstore.Store {
		t.Helper()
		store, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}

func TestOpenDetectsCorruptedBytes(t *testing.T) {
	directory := t.TempDir()
	store, err := New(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key, err := blobstore.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.Put(context.Background(), key, bytes.NewReader([]byte("trusted")), blobstore.PutOptions{
		MaxBytes: 64,
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	dataName := key.String() + "." + metadata.Version + ".blob"
	if err := os.WriteFile(filepath.Join(directory, dataName), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader, _, err := store.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(reader)
	_ = reader.Close()
	if !errors.Is(err, blobstore.ErrIntegrity) {
		t.Fatalf("ReadAll() error = %v, want ErrIntegrity", err)
	}
}

func TestOpenRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	store, err := New(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key, err := blobstore.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.Put(context.Background(), key, bytes.NewReader([]byte("trusted")), blobstore.PutOptions{
		MaxBytes: 64,
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	dataName := key.String() + "." + metadata.Version + ".blob"
	if err := os.Remove(filepath.Join(directory, dataName)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(key.String()+".manifest.json", filepath.Join(directory, dataName)); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, _, err := store.Open(context.Background(), key); !errors.Is(err, blobstore.ErrIntegrity) {
		t.Fatalf("Open(symlink) error = %v, want ErrIntegrity", err)
	}
}
