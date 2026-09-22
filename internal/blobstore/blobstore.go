// Package blobstore defines backend-neutral immutable blob storage.
package blobstore

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
)

var (
	ErrAlreadyExists  = errors.New("blob already exists")
	ErrBackend        = errors.New("blob store backend failure")
	ErrDeadline       = errors.New("blob operation deadline exceeded")
	ErrIntegrity      = errors.New("blob integrity verification failed")
	ErrInvalidKey     = errors.New("invalid blob key")
	ErrInvalidOptions = errors.New("invalid blob options")
	ErrNotFound       = errors.New("blob not found")
	ErrTooLarge       = errors.New("blob exceeds byte limit")
	ErrVersion        = errors.New("blob version conflict")
)

var keyPattern = regexp.MustCompile(`^blb_[a-z2-7]{52}$`)

// Key is an opaque, server-generated storage identifier.
type Key struct {
	value string
}

// NewKey creates a cryptographically random object key.
func NewKey() (Key, error) {
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return Key{}, fmt.Errorf("generate blob key: %w", ErrBackend)
	}
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(random)
	return Key{value: "blb_" + stringLower(encoded)}, nil
}

// ParseKey restores a previously generated key from trusted persistence while
// rejecting paths, separators, and arbitrary client values.
func ParseKey(value string) (Key, error) {
	if !keyPattern.MatchString(value) {
		return Key{}, ErrInvalidKey
	}
	return Key{value: value}, nil
}

// String returns the opaque representation for persistence, never a path.
func (k Key) String() string {
	return k.value
}

// Valid reports whether the key has the generated-key representation.
func (k Key) Valid() bool {
	return keyPattern.MatchString(k.value)
}

// Metadata describes committed immutable bytes.
type Metadata struct {
	Key            Key
	Size           int64
	SHA256         string
	Version        string
	BackendVersion string
	CommittedAt    time.Time
}

// PutOptions bounds and optionally verifies a streaming write.
type PutOptions struct {
	MaxBytes       int64
	Timeout        time.Duration
	ExpectedSize   *int64
	ExpectedSHA256 string
}

// Store persists immutable blobs. Open verifies the stream against committed
// metadata and reports ErrIntegrity instead of io.EOF when verification fails.
// Callers must consume streams to EOF and close them.
type Store interface {
	Put(context.Context, Key, io.Reader, PutOptions) (Metadata, error)
	Open(context.Context, Key) (io.ReadCloser, Metadata, error)
	Stat(context.Context, Key) (Metadata, error)
	Delete(context.Context, Key, string) error
	Close() error
}

// ValidatePutOptions checks limits before any backend I/O.
func ValidatePutOptions(key Key, source io.Reader, options PutOptions) error {
	if !key.Valid() {
		return ErrInvalidKey
	}
	if source == nil || options.MaxBytes <= 0 || options.Timeout <= 0 {
		return ErrInvalidOptions
	}
	if options.ExpectedSize != nil && (*options.ExpectedSize < 0 || *options.ExpectedSize > options.MaxBytes) {
		return ErrInvalidOptions
	}
	if options.ExpectedSHA256 != "" && !validSHA256(options.ExpectedSHA256) {
		return ErrInvalidOptions
	}
	return nil
}

// BackendError returns a safe error that does not contain credentials, bucket
// names, endpoints, or filesystem paths.
func BackendError(operation string) error {
	return fmt.Errorf("%w: %s", ErrBackend, operation)
}

// ContextError normalizes cancellation and deadlines without backend details.
func ContextError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ErrDeadline
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

// ValidSHA256 reports whether value is a lowercase hex SHA-256 digest.
func ValidSHA256(value string) bool {
	return validSHA256(value)
}

func stringLower(value string) string {
	buffer := []byte(value)
	for index, char := range buffer {
		if char >= 'A' && char <= 'Z' {
			buffer[index] = char + ('a' - 'A')
		}
	}
	return string(buffer)
}
