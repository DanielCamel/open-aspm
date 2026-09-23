package ingestion

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spectremi/open-aspm/internal/blobstore"
)

// AuthorizationRequest is the complete capability and resource scope checked
// before the ingestion repository is called.
type AuthorizationRequest struct {
	PrincipalID   string
	WorkspaceID   string
	ApplicationID string
	Capability    string
}

// Authorizer evaluates current principal, membership, capability, token, and
// application scope. Authentication alone is not authorization.
type Authorizer interface {
	Authorize(context.Context, AuthorizationRequest) error
}

type reservationStore interface {
	Reserve(context.Context, reserveSpec) (Reservation, error)
}

type ingestionStore interface {
	reservationStore
	BeginUpload(context.Context, beginUploadSpec) (uploadSession, error)
	CommitUpload(context.Context, commitUploadSpec) (UploadReceipt, error)
	EndUpload(context.Context, endUploadSpec) error
}

type blobWriter interface {
	Put(context.Context, blobstore.Key, io.Reader, blobstore.PutOptions) (blobstore.Metadata, error)
	Stat(context.Context, blobstore.Key) (blobstore.Metadata, error)
}

// Config controls server-selected reservation policy. Idempotency retention
// must never be shorter than the public v1 contract's 24-hour minimum.
type Config struct {
	MaxUploadBytes       int64
	UploadReservationTTL time.Duration
	UploadTimeout        time.Duration
	IdempotencyRetention time.Duration
	StorageBackend       string
}

// Service authorizes import reservation and raw-artifact upload through the
// ingestion-owned store.
type Service struct {
	store      ingestionStore
	blobs      blobWriter
	authorizer Authorizer
	config     Config
	now        func() time.Time
	random     func([]byte) error
	newBlobKey func() (blobstore.Key, error)
}

// NewService validates dependencies and policy.
func NewService(store ingestionStore, blobs blobWriter, authorizer Authorizer, config Config) (*Service, error) {
	if store == nil || blobs == nil || authorizer == nil || config.MaxUploadBytes <= 0 ||
		config.UploadReservationTTL <= 0 || config.UploadTimeout <= 0 ||
		config.IdempotencyRetention < minimumRetention ||
		!storageBackendPattern.MatchString(config.StorageBackend) {
		return nil, ErrInvalid
	}
	return &Service{
		store: store, blobs: blobs, authorizer: authorizer, config: config,
		now: time.Now,
		random: func(buffer []byte) error {
			_, err := rand.Read(buffer)
			return err
		},
		newBlobKey: blobstore.NewKey,
	}, nil
}

// Reserve authorizes createImport, validates its semantic request, and records
// one durable reservation. Identical scoped retries replay the original Import.
func (service *Service) Reserve(ctx context.Context, request ReserveRequest) (Reservation, error) {
	if err := validateAuthorizationScope(request); err != nil {
		return Reservation{}, err
	}
	if err := service.authorizer.Authorize(ctx, AuthorizationRequest{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, Capability: CapabilityImportsCreate,
	}); err != nil {
		return Reservation{}, fmt.Errorf("authorize import reservation: %w", err)
	}
	fingerprint, err := fingerprintRequest(request)
	if err != nil {
		return Reservation{}, err
	}

	importID, err := service.randomIdentifier("imp_")
	if err != nil {
		return Reservation{}, err
	}
	artifactID, err := service.randomIdentifier("art_")
	if err != nil {
		return Reservation{}, err
	}
	storageKey, err := service.newBlobKey()
	if err != nil {
		return Reservation{}, fmt.Errorf("generate raw artifact storage key: %w", err)
	}
	now := service.timestamp()
	expectedSize := request.ExpectedSize
	if expectedSize != nil {
		copied := *expectedSize
		expectedSize = &copied
	}
	created := Import{
		ID: importID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, CreatedByPrincipalID: request.PrincipalID,
		State: ImportAwaitingUpload, ReportFormat: request.ReportFormat,
		OriginalFilename: request.OriginalFilename, ExpectedSize: expectedSize,
		ExpectedSHA256: request.ExpectedSHA256, MaxBytes: service.config.MaxUploadBytes,
		UploadExpiresAt: now.Add(service.config.UploadReservationTTL),
		CreatedAt:       now, UpdatedAt: now,
	}
	return service.store.Reserve(ctx, reserveSpec{
		Import: created, ArtifactID: artifactID, StorageBackend: service.config.StorageBackend,
		StorageKey: storageKey.String(), PrincipalID: request.PrincipalID, APIMajorVersion: apiMajorVersion,
		Operation: operationCreateImport, IdempotencyKey: request.IdempotencyKey,
		RequestFingerprint:   fingerprint,
		RequestExceedsLimit:  request.ExpectedSize != nil && *request.ExpectedSize > service.config.MaxUploadBytes,
		IdempotencyExpiresAt: now.Add(service.config.IdempotencyRetention),
	})
}

// Upload authorizes a reserved content resource, streams it into immutable
// storage, and commits verified evidence metadata. The database lease prevents
// concurrent attempts from publishing competing content; BlobStore remains the
// immutable commit boundary when database and object-store outcomes diverge.
func (service *Service) Upload(ctx context.Context, request UploadRequest) (UploadReceipt, error) {
	if err := validateUploadRequest(request); err != nil {
		return UploadReceipt{}, err
	}
	if err := service.authorizer.Authorize(ctx, AuthorizationRequest{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, Capability: CapabilityImportsUpload,
	}); err != nil {
		return UploadReceipt{}, fmt.Errorf("authorize import upload: %w", err)
	}

	artifactID, err := service.randomIdentifier("art_")
	if err != nil {
		return UploadReceipt{}, err
	}
	attemptID, err := service.randomIdentifier("upl_")
	if err != nil {
		return UploadReceipt{}, err
	}
	key, err := service.newBlobKey()
	if err != nil {
		return UploadReceipt{}, fmt.Errorf("generate raw artifact storage key: %w", err)
	}
	now := service.timestamp()
	session, err := service.store.BeginUpload(ctx, beginUploadSpec{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, ImportID: request.ImportID,
		ArtifactID: artifactID, StorageBackend: service.config.StorageBackend,
		StorageKey: key.String(), AttemptID: attemptID, StartedAt: now,
		LeaseExpiresAt: now.Add(service.config.UploadTimeout),
	})
	if err != nil {
		return UploadReceipt{}, err
	}
	if session.Committed {
		if err := service.verifyReplay(ctx, request, session); err != nil {
			return UploadReceipt{}, err
		}
		return session.Receipt, nil
	}
	if session.StorageBackend != service.config.StorageBackend {
		service.releaseAttempt(session, service.timestamp())
		return UploadReceipt{}, ErrUploadStorage
	}

	if request.DeclaredSize != nil && *request.DeclaredSize > session.MaxBytes {
		return UploadReceipt{}, service.rejectAttempt(session, service.timestamp(), "size_limit_exceeded", ErrTooLarge)
	}
	if request.DeclaredSize != nil && session.ExpectedSize != nil &&
		*request.DeclaredSize != *session.ExpectedSize {
		return UploadReceipt{}, service.rejectAttempt(session, service.timestamp(), "size_mismatch", ErrUploadMismatch)
	}
	storageKey, err := parseStorageKey(session.StorageKey)
	if err != nil {
		service.releaseAttempt(session, service.timestamp())
		return UploadReceipt{}, err
	}

	uploadCtx, cancel := context.WithTimeout(ctx, service.config.UploadTimeout)
	defer cancel()
	metadata, err := service.blobs.Stat(uploadCtx, storageKey)
	switch {
	case err == nil:
		if verifyErr := verifyContent(uploadCtx, request.Content, session.MaxBytes,
			request.DeclaredSize, session.ExpectedSize, session.ExpectedSHA256, &metadata); verifyErr != nil {
			service.releaseAttempt(session, service.timestamp())
			return UploadReceipt{}, verifyErr
		}
	case errors.Is(err, blobstore.ErrNotFound):
		expectedSize := session.ExpectedSize
		if request.DeclaredSize != nil {
			expectedSize = request.DeclaredSize
		}
		metadata, err = service.blobs.Put(uploadCtx, storageKey, request.Content, blobstore.PutOptions{
			MaxBytes: session.MaxBytes, Timeout: service.config.UploadTimeout,
			ExpectedSize: expectedSize, ExpectedSHA256: session.ExpectedSHA256,
		})
		if err != nil {
			return UploadReceipt{}, service.handleBlobWriteFailure(session, service.timestamp(), err)
		}
	default:
		service.releaseAttempt(session, service.timestamp())
		return UploadReceipt{}, fmt.Errorf("inspect raw artifact storage: %w", err)
	}

	receipt, err := service.store.CommitUpload(ctx, commitUploadSpec{
		WorkspaceID: request.WorkspaceID, ImportID: request.ImportID,
		ArtifactID: session.ArtifactID, AttemptID: session.AttemptID,
		SizeBytes: metadata.Size, SHA256: metadata.SHA256,
		StorageVersion: metadata.Version, BackendVersion: metadata.BackendVersion,
		CommittedAt: metadata.CommittedAt.UTC().Truncate(time.Microsecond),
	})
	if err != nil {
		service.releaseAttempt(session, service.timestamp())
		return UploadReceipt{}, fmt.Errorf("commit raw artifact metadata: %w", err)
	}
	return receipt, nil
}

func (service *Service) verifyReplay(ctx context.Context, request UploadRequest, session uploadSession) error {
	verifyCtx, cancel := context.WithTimeout(ctx, service.config.UploadTimeout)
	defer cancel()
	metadata := blobstore.Metadata{
		Size: session.Receipt.SizeBytes, SHA256: session.Receipt.SHA256,
		CommittedAt: session.Receipt.UploadedAt,
	}
	if err := verifyContent(verifyCtx, request.Content, session.MaxBytes, request.DeclaredSize,
		session.ExpectedSize, session.ExpectedSHA256, &metadata); err != nil {
		return ErrUploadConflict
	}
	return nil
}

func verifyContent(
	ctx context.Context,
	content io.Reader,
	maxBytes int64,
	declaredSize, expectedSize *int64,
	expectedSHA256 string,
	committed *blobstore.Metadata,
) error {
	if declaredSize != nil && (*declaredSize > maxBytes || *declaredSize != committed.Size) {
		return ErrUploadConflict
	}
	if expectedSize != nil && *expectedSize != committed.Size {
		return ErrUploadMismatch
	}
	if expectedSHA256 != "" && expectedSHA256 != committed.SHA256 {
		return ErrUploadMismatch
	}
	bounded := blobstore.NewBoundedReader(ctx, content, maxBytes)
	if _, err := io.Copy(io.Discard, bounded); err != nil {
		if errors.Is(err, blobstore.ErrTooLarge) {
			return ErrUploadConflict
		}
		return err
	}
	size, digest, err := bounded.Result()
	if err != nil {
		return err
	}
	if size != committed.Size || digest != committed.SHA256 {
		return ErrUploadConflict
	}
	return nil
}

func (service *Service) handleBlobWriteFailure(session uploadSession, endedAt time.Time, err error) error {
	switch {
	case errors.Is(err, blobstore.ErrTooLarge):
		return service.rejectAttempt(session, endedAt, "size_limit_exceeded", ErrTooLarge)
	case errors.Is(err, blobstore.ErrIntegrity):
		return service.rejectAttempt(session, endedAt, "content_mismatch", ErrUploadMismatch)
	case errors.Is(err, blobstore.ErrAlreadyExists):
		service.releaseAttempt(session, endedAt)
		return ErrUploadInProgress
	default:
		service.releaseAttempt(session, endedAt)
		return fmt.Errorf("store raw artifact: %w", err)
	}
}

func (service *Service) rejectAttempt(
	session uploadSession,
	endedAt time.Time,
	code string,
	cause error,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := service.store.EndUpload(ctx, endUploadSpec{
		WorkspaceID: session.WorkspaceID, ImportID: session.ImportID,
		ArtifactID: session.ArtifactID, AttemptID: session.AttemptID,
		EndedAt: endedAt, RejectCode: code,
	}); err != nil {
		return errors.Join(cause, fmt.Errorf("record rejected upload: %w", err))
	}
	return cause
}

func (service *Service) releaseAttempt(session uploadSession, endedAt time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = service.store.EndUpload(ctx, endUploadSpec{
		WorkspaceID: session.WorkspaceID, ImportID: session.ImportID, ArtifactID: session.ArtifactID,
		AttemptID: session.AttemptID, EndedAt: endedAt,
	})
}

func (service *Service) randomIdentifier(prefix string) (string, error) {
	random := make([]byte, 20)
	if err := service.random(random); err != nil {
		return "", fmt.Errorf("generate ingestion identifier: %w", err)
	}
	return encodeRandomID(prefix, random), nil
}

func (service *Service) timestamp() time.Time {
	return service.now().UTC().Truncate(time.Microsecond)
}
