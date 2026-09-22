// Package s3store implements BlobStore on an S3-compatible object service.
package s3store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"io"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	transfertypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/spectremi/open-aspm/internal/blobstore"
)

const maxManifestBytes = 16 << 10

// Config identifies an operator-controlled private bucket and prefix.
type Config struct {
	Bucket     string
	Prefix     string
	Encryption string
	KMSKeyID   string
}

// Store is an S3-compatible immutable blob store.
type Store struct {
	client   *s3.Client
	uploader *transfermanager.Client
	config   Config
}

type manifest struct {
	Key            string    `json:"key"`
	DataKey        string    `json:"data_key"`
	Size           int64     `json:"size"`
	SHA256         string    `json:"sha256"`
	Version        string    `json:"version"`
	BackendVersion string    `json:"backend_version,omitempty"`
	CommittedAt    time.Time `json:"committed_at"`
}

// New creates an adapter around an already configured AWS SDK client. The
// caller owns credential loading and TLS policy; credentials are never copied
// into domain metadata or errors.
func New(client *s3.Client, config Config) (*Store, error) {
	if client == nil || config.Bucket == "" {
		return nil, blobstore.ErrInvalidOptions
	}
	prefix := strings.Trim(config.Prefix, "/")
	if strings.Contains(prefix, "\\") || prefix == "." || prefix == ".." ||
		strings.HasPrefix(prefix, "../") || strings.Contains(prefix, "/../") {
		return nil, blobstore.ErrInvalidOptions
	}
	if prefix != "" && path.Clean(prefix) != prefix {
		return nil, blobstore.ErrInvalidOptions
	}
	switch config.Encryption {
	case "", "AES256":
		if config.KMSKeyID != "" {
			return nil, blobstore.ErrInvalidOptions
		}
	case "aws:kms":
		if config.KMSKeyID == "" {
			return nil, blobstore.ErrInvalidOptions
		}
	default:
		return nil, blobstore.ErrInvalidOptions
	}
	config.Prefix = prefix
	uploader := transfermanager.New(client, func(options *transfermanager.Options) {
		options.ChecksumAlgorithm = transfertypes.ChecksumAlgorithmSha256
		options.Concurrency = 2
		options.PartSizeBytes = 8 << 20
		options.MultipartUploadThreshold = 16 << 20
	})
	return &Store{client: client, uploader: uploader, config: config}, nil
}

// Put streams bytes through the bounded transfer manager and publishes a
// small manifest as the commit marker.
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

	if _, err := store.readManifest(ctx, key); err == nil {
		return blobstore.Metadata{}, blobstore.ErrAlreadyExists
	} else if !errors.Is(err, blobstore.ErrNotFound) {
		return blobstore.Metadata{}, err
	}
	version, err := randomID(20)
	if err != nil {
		return blobstore.Metadata{}, err
	}
	dataKey := store.dataKey(key, version)
	bounded := blobstore.NewBoundedReader(ctx, source, options.MaxBytes)
	upload, err := store.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket:               aws.String(store.config.Bucket),
		Key:                  aws.String(dataKey),
		Body:                 bounded,
		ChecksumAlgorithm:    transfertypes.ChecksumAlgorithmSha256,
		ServerSideEncryption: transfertypes.ServerSideEncryption(store.config.Encryption),
		SSEKMSKeyID:          optionalString(store.config.KMSKeyID),
	})
	if err != nil {
		if streamErr := normalizeStreamError(ctx, bounded.Err()); streamErr != nil {
			return blobstore.Metadata{}, streamErr
		}
		return blobstore.Metadata{}, safeS3Error("upload object", err)
	}
	size, digest, err := bounded.Result()
	if err != nil {
		store.cleanupData(dataKey, aws.ToString(upload.VersionID))
		return blobstore.Metadata{}, normalizeStreamError(ctx, err)
	}
	if err := blobstore.VerifyExpected(size, digest, options); err != nil {
		store.cleanupData(dataKey, aws.ToString(upload.VersionID))
		return blobstore.Metadata{}, err
	}
	record := manifest{
		Key:            key.String(),
		DataKey:        dataKey,
		Size:           size,
		SHA256:         digest,
		Version:        version,
		BackendVersion: aws.ToString(upload.VersionID),
		CommittedAt:    time.Now().UTC(),
	}
	if err := store.commitManifest(ctx, record); err != nil {
		store.cleanupData(dataKey, record.BackendVersion)
		return blobstore.Metadata{}, err
	}
	return metadata(key, record), nil
}

// Open returns a stream verified by both the SDK checksum middleware and the
// committed Open ASPM SHA-256 manifest.
func (store *Store) Open(ctx context.Context, key blobstore.Key) (io.ReadCloser, blobstore.Metadata, error) {
	if !key.Valid() {
		return nil, blobstore.Metadata{}, blobstore.ErrInvalidKey
	}
	record, err := store.readManifest(ctx, key)
	if err != nil {
		return nil, blobstore.Metadata{}, err
	}
	output, err := store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(store.config.Bucket),
		Key:          aws.String(record.DataKey),
		VersionId:    optionalString(record.BackendVersion),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		if isNotFound(err) {
			return nil, blobstore.Metadata{}, blobstore.ErrIntegrity
		}
		return nil, blobstore.Metadata{}, safeS3Error("open object", err)
	}
	return blobstore.NewVerifiedReader(
		ctx,
		&safeReadCloser{source: output.Body},
		record.Size,
		record.SHA256,
	), metadata(key, record), nil
}

// Stat reads only the small commit manifest.
func (store *Store) Stat(ctx context.Context, key blobstore.Key) (blobstore.Metadata, error) {
	if !key.Valid() {
		return blobstore.Metadata{}, blobstore.ErrInvalidKey
	}
	record, err := store.readManifest(ctx, key)
	if err != nil {
		return blobstore.Metadata{}, err
	}
	return metadata(key, record), nil
}

// Delete removes the expected object version. Missing objects are a success.
func (store *Store) Delete(ctx context.Context, key blobstore.Key, expectedVersion string) error {
	if !key.Valid() {
		return blobstore.ErrInvalidKey
	}
	if expectedVersion == "" {
		return blobstore.ErrInvalidOptions
	}
	record, err := store.readManifest(ctx, key)
	if errors.Is(err, blobstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if record.Version != expectedVersion {
		return blobstore.ErrVersion
	}
	if err := store.deleteData(ctx, record.DataKey, record.BackendVersion); err != nil {
		return err
	}
	_, err = store.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(store.config.Bucket),
		Key:    aws.String(store.manifestKey(key)),
	})
	if err != nil {
		return safeS3Error("delete object manifest", err)
	}
	return nil
}

// Close is a no-op; the AWS SDK client owns its HTTP transport lifecycle.
func (store *Store) Close() error {
	return nil
}

func (store *Store) commitManifest(ctx context.Context, record manifest) error {
	content, err := json.Marshal(record)
	if err != nil {
		return blobstore.BackendError("encode object manifest")
	}
	_, err = store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(store.config.Bucket),
		Key:                  aws.String(store.manifestKeyFromString(record.Key)),
		Body:                 bytes.NewReader(content),
		ContentLength:        aws.Int64(int64(len(content))),
		ContentType:          aws.String("application/json"),
		IfNoneMatch:          aws.String("*"),
		ChecksumAlgorithm:    types.ChecksumAlgorithmSha256,
		ServerSideEncryption: types.ServerSideEncryption(store.config.Encryption),
		SSEKMSKeyId:          optionalString(store.config.KMSKeyID),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return blobstore.ErrAlreadyExists
		}
		return safeS3Error("commit object manifest", err)
	}
	return nil
}

func (store *Store) readManifest(ctx context.Context, key blobstore.Key) (manifest, error) {
	output, err := store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(store.config.Bucket),
		Key:          aws.String(store.manifestKey(key)),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		if isNotFound(err) {
			return manifest{}, blobstore.ErrNotFound
		}
		return manifest{}, safeS3Error("read object manifest", err)
	}
	defer output.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(output.Body, maxManifestBytes+1))
	decoder.DisallowUnknownFields()
	var record manifest
	if output.ContentLength != nil && *output.ContentLength > maxManifestBytes {
		return manifest{}, blobstore.ErrIntegrity
	}
	if err := decoder.Decode(&record); err != nil {
		return manifest{}, blobstore.ErrIntegrity
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return manifest{}, blobstore.ErrIntegrity
	}
	if record.Key != key.String() || record.Version == "" ||
		record.DataKey != store.dataKey(key, record.Version) ||
		record.Size < 0 || !blobstore.ValidSHA256(record.SHA256) || record.CommittedAt.IsZero() {
		return manifest{}, blobstore.ErrIntegrity
	}
	return record, nil
}

func (store *Store) deleteData(ctx context.Context, dataKey, backendVersion string) error {
	_, err := store.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(store.config.Bucket),
		Key:       aws.String(dataKey),
		VersionId: optionalString(backendVersion),
	})
	if err != nil && !isNotFound(err) {
		return safeS3Error("delete object bytes", err)
	}
	return nil
}

func (store *Store) cleanupData(dataKey, backendVersion string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = store.deleteData(ctx, dataKey, backendVersion)
}

func (store *Store) dataKey(key blobstore.Key, version string) string {
	return store.join("objects", key.String(), version, "data")
}

func (store *Store) manifestKey(key blobstore.Key) string {
	return store.manifestKeyFromString(key.String())
}

func (store *Store) manifestKeyFromString(key string) string {
	return store.join("objects", key, "manifest.json")
}

func (store *Store) join(parts ...string) string {
	if store.config.Prefix == "" {
		return path.Join(parts...)
	}
	return path.Join(append([]string{store.config.Prefix}, parts...)...)
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

func randomID(byteCount int) (string, error) {
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		return "", blobstore.BackendError("generate object version")
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(value)), nil
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return aws.String(value)
}

func normalizeStreamError(ctx context.Context, err error) error {
	if contextErr := blobstore.ContextError(ctx); contextErr != nil {
		return contextErr
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, blobstore.ErrTooLarge) || errors.Is(err, blobstore.ErrIntegrity) {
		return err
	}
	return blobstore.BackendError("stream object")
}

func safeS3Error(operation string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return blobstore.ErrDeadline
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	return blobstore.BackendError(operation)
}

func isNotFound(err error) bool {
	var apiError smithy.APIError
	return errors.As(err, &apiError) &&
		(apiError.ErrorCode() == "NoSuchKey" || apiError.ErrorCode() == "NotFound" || apiError.ErrorCode() == "NoSuchVersion")
}

func isPreconditionFailed(err error) bool {
	var apiError smithy.APIError
	return errors.As(err, &apiError) && apiError.ErrorCode() == "PreconditionFailed"
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
