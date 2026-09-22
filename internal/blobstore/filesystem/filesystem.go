// Package filesystem implements BlobStore on a controlled local directory.
package filesystem

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spectremi/open-aspm/internal/blobstore"
)

const maxManifestBytes = 16 << 10

// Store is a filesystem-backed blob store rooted by os.Root.
type Store struct {
	root *os.Root
}

type manifest struct {
	Key            string    `json:"key"`
	DataName       string    `json:"data_name"`
	Size           int64     `json:"size"`
	SHA256         string    `json:"sha256"`
	Version        string    `json:"version"`
	BackendVersion string    `json:"backend_version,omitempty"`
	CommittedAt    time.Time `json:"committed_at"`
}

// New creates a filesystem store. The root must be an absolute, existing
// directory and must not be the filesystem root or current working directory.
func New(directory string) (*Store, error) {
	if !filepath.IsAbs(directory) {
		return nil, errors.New("blob root must be an absolute path")
	}
	clean := filepath.Clean(directory)
	volumeRoot := filepath.Clean(filepath.VolumeName(clean) + string(os.PathSeparator))
	if clean == volumeRoot {
		return nil, errors.New("blob root must not be a filesystem root")
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		return nil, blobstore.BackendError("resolve working directory")
	}
	if clean == filepath.Clean(workingDirectory) {
		return nil, errors.New("blob root must not be the current working directory")
	}
	info, err := os.Stat(clean)
	if err != nil || !info.IsDir() {
		return nil, errors.New("blob root must be an existing directory")
	}
	root, err := os.OpenRoot(clean)
	if err != nil {
		return nil, blobstore.BackendError("open filesystem root")
	}
	return &Store{root: root}, nil
}

// Put streams immutable bytes and publishes a manifest only after verification.
func (store *Store) Put(
	ctx context.Context,
	key blobstore.Key,
	source io.Reader,
	options blobstore.PutOptions,
) (blobstore.Metadata, error) {
	if err := blobstore.ValidatePutOptions(key, source, options); err != nil {
		return blobstore.Metadata{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	if _, err := store.readManifest(key); err == nil {
		return blobstore.Metadata{}, blobstore.ErrAlreadyExists
	} else if !errors.Is(err, blobstore.ErrNotFound) {
		return blobstore.Metadata{}, err
	}

	version, err := randomID(20)
	if err != nil {
		return blobstore.Metadata{}, err
	}
	temporaryName := ".staging-" + version
	dataName := key.String() + "." + version + ".blob"
	file, err := store.root.OpenFile(temporaryName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return blobstore.Metadata{}, blobstore.BackendError("create staging object")
	}
	removeTemporary := true
	defer func() {
		_ = file.Close()
		if removeTemporary {
			_ = store.root.Remove(temporaryName)
		}
	}()

	bounded := blobstore.NewBoundedReader(ctx, source, options.MaxBytes)
	if _, err := io.Copy(file, bounded); err != nil {
		return blobstore.Metadata{}, normalizeStreamError(ctx, err)
	}
	size, digest, err := bounded.Result()
	if err != nil {
		return blobstore.Metadata{}, normalizeStreamError(ctx, err)
	}
	if err := blobstore.VerifyExpected(size, digest, options); err != nil {
		return blobstore.Metadata{}, err
	}
	if err := blobstore.ContextError(ctx); err != nil {
		return blobstore.Metadata{}, err
	}
	if err := file.Sync(); err != nil {
		return blobstore.Metadata{}, blobstore.BackendError("sync staging object")
	}
	if err := blobstore.ContextError(ctx); err != nil {
		return blobstore.Metadata{}, err
	}
	if err := file.Close(); err != nil {
		return blobstore.Metadata{}, blobstore.BackendError("close staging object")
	}
	if err := store.root.Rename(temporaryName, dataName); err != nil {
		return blobstore.Metadata{}, blobstore.BackendError("commit object bytes")
	}
	removeTemporary = false
	removeData := true
	defer func() {
		if removeData {
			_ = store.root.Remove(dataName)
		}
	}()

	record := manifest{
		Key:         key.String(),
		DataName:    dataName,
		Size:        size,
		SHA256:      digest,
		Version:     version,
		CommittedAt: time.Now().UTC(),
	}
	if err := store.commitManifest(record); err != nil {
		return blobstore.Metadata{}, err
	}
	removeData = false
	return metadata(key, record), nil
}

// Open returns a stream that validates size and SHA-256 at EOF.
func (store *Store) Open(ctx context.Context, key blobstore.Key) (io.ReadCloser, blobstore.Metadata, error) {
	if !key.Valid() {
		return nil, blobstore.Metadata{}, blobstore.ErrInvalidKey
	}
	record, err := store.readManifest(key)
	if err != nil {
		return nil, blobstore.Metadata{}, err
	}
	info, err := store.root.Lstat(record.DataName)
	if err != nil || !info.Mode().IsRegular() {
		return nil, blobstore.Metadata{}, blobstore.ErrIntegrity
	}
	file, err := store.root.Open(record.DataName)
	if err != nil {
		return nil, blobstore.Metadata{}, blobstore.ErrIntegrity
	}
	return blobstore.NewVerifiedReader(
		ctx,
		&safeReadCloser{source: file},
		record.Size,
		record.SHA256,
	), metadata(key, record), nil
}

// Stat returns committed metadata without opening report content.
func (store *Store) Stat(ctx context.Context, key blobstore.Key) (blobstore.Metadata, error) {
	if err := blobstore.ContextError(ctx); err != nil {
		return blobstore.Metadata{}, err
	}
	if !key.Valid() {
		return blobstore.Metadata{}, blobstore.ErrInvalidKey
	}
	record, err := store.readManifest(key)
	if err != nil {
		return blobstore.Metadata{}, err
	}
	return metadata(key, record), nil
}

// Delete removes the expected immutable version. Missing objects are a success.
func (store *Store) Delete(ctx context.Context, key blobstore.Key, expectedVersion string) error {
	if err := blobstore.ContextError(ctx); err != nil {
		return err
	}
	if !key.Valid() {
		return blobstore.ErrInvalidKey
	}
	if expectedVersion == "" {
		return blobstore.ErrInvalidOptions
	}
	record, err := store.readManifest(key)
	if errors.Is(err, blobstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if record.Version != expectedVersion {
		return blobstore.ErrVersion
	}
	if err := store.root.Remove(record.DataName); err != nil && !errors.Is(err, os.ErrNotExist) {
		return blobstore.BackendError("delete object bytes")
	}
	if err := store.root.Remove(manifestName(key)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return blobstore.BackendError("delete object manifest")
	}
	return nil
}

// Close releases the root directory handle.
func (store *Store) Close() error {
	return store.root.Close()
}

func (store *Store) commitManifest(record manifest) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return blobstore.BackendError("encode object manifest")
	}
	temporaryName := ".manifest-" + record.Version
	file, err := store.root.OpenFile(temporaryName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return blobstore.BackendError("create object manifest")
	}
	defer func() {
		_ = file.Close()
		_ = store.root.Remove(temporaryName)
	}()
	if _, err := file.Write(encoded); err != nil {
		return blobstore.BackendError("write object manifest")
	}
	if err := file.Sync(); err != nil {
		return blobstore.BackendError("sync object manifest")
	}
	if err := file.Close(); err != nil {
		return blobstore.BackendError("close object manifest")
	}
	if err := store.root.Link(temporaryName, manifestNameFromString(record.Key)); err != nil {
		if _, statErr := store.root.Lstat(manifestNameFromString(record.Key)); statErr == nil {
			return blobstore.ErrAlreadyExists
		}
		return blobstore.BackendError("commit object manifest")
	}
	return nil
}

func (store *Store) readManifest(key blobstore.Key) (manifest, error) {
	name := manifestName(key)
	info, err := store.root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return manifest{}, blobstore.ErrNotFound
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxManifestBytes {
		return manifest{}, blobstore.ErrIntegrity
	}
	file, err := store.root.Open(name)
	if err != nil {
		return manifest{}, blobstore.BackendError("open object manifest")
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxManifestBytes+1))
	decoder.DisallowUnknownFields()
	var record manifest
	if err := decoder.Decode(&record); err != nil {
		return manifest{}, blobstore.ErrIntegrity
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return manifest{}, blobstore.ErrIntegrity
	}
	if record.Key != key.String() || record.Version == "" ||
		record.DataName != key.String()+"."+record.Version+".blob" ||
		record.Size < 0 || !blobstore.ValidSHA256(record.SHA256) || record.CommittedAt.IsZero() {
		return manifest{}, blobstore.ErrIntegrity
	}
	return record, nil
}

func metadata(key blobstore.Key, record manifest) blobstore.Metadata {
	return blobstore.Metadata{
		Key:            key,
		Size:           record.Size,
		SHA256:         record.SHA256,
		Version:        record.Version,
		BackendVersion: record.BackendVersion,
		CommittedAt:    record.CommittedAt,
	}
}

func manifestName(key blobstore.Key) string {
	return manifestNameFromString(key.String())
}

func manifestNameFromString(key string) string {
	return key + ".manifest.json"
}

func randomID(byteCount int) (string, error) {
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		return "", blobstore.BackendError("generate object version")
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(value)), nil
}

func normalizeStreamError(ctx context.Context, err error) error {
	if contextErr := blobstore.ContextError(ctx); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, blobstore.ErrTooLarge) || errors.Is(err, blobstore.ErrIntegrity) {
		return err
	}
	return blobstore.BackendError("stream object")
}

type safeReadCloser struct {
	source io.ReadCloser
}

func (reader *safeReadCloser) Read(buffer []byte) (int, error) {
	count, err := reader.source.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		return count, blobstore.BackendError("read object")
	}
	return count, err
}

func (reader *safeReadCloser) Close() error {
	if err := reader.source.Close(); err != nil {
		return blobstore.BackendError("close object")
	}
	return nil
}

var _ blobstore.Store = (*Store)(nil)
