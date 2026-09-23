package ingestion

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"
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

// Config controls server-selected reservation policy. Idempotency retention
// must never be shorter than the public v1 contract's 24-hour minimum.
type Config struct {
	MaxUploadBytes       int64
	UploadReservationTTL time.Duration
	IdempotencyRetention time.Duration
}

// Service authorizes and reserves imports through the ingestion-owned store.
type Service struct {
	store      reservationStore
	authorizer Authorizer
	config     Config
	now        func() time.Time
	random     func([]byte) error
}

// NewService validates dependencies and policy.
func NewService(store reservationStore, authorizer Authorizer, config Config) (*Service, error) {
	if store == nil || authorizer == nil || config.MaxUploadBytes <= 0 ||
		config.UploadReservationTTL <= 0 || config.IdempotencyRetention < minimumRetention {
		return nil, ErrInvalid
	}
	return &Service{
		store: store, authorizer: authorizer, config: config,
		now: time.Now,
		random: func(buffer []byte) error {
			_, err := rand.Read(buffer)
			return err
		},
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

	random := make([]byte, 20)
	if err := service.random(random); err != nil {
		return Reservation{}, fmt.Errorf("generate import identifier: %w", err)
	}
	now := service.now().UTC().Truncate(time.Microsecond)
	expectedSize := request.ExpectedSize
	if expectedSize != nil {
		copied := *expectedSize
		expectedSize = &copied
	}
	created := Import{
		ID: encodeRandomID("imp_", random), WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, CreatedByPrincipalID: request.PrincipalID,
		State: ImportAwaitingUpload, ReportFormat: request.ReportFormat,
		OriginalFilename: request.OriginalFilename, ExpectedSize: expectedSize,
		ExpectedSHA256: request.ExpectedSHA256, MaxBytes: service.config.MaxUploadBytes,
		UploadExpiresAt: now.Add(service.config.UploadReservationTTL),
		CreatedAt:       now, UpdatedAt: now,
	}
	return service.store.Reserve(ctx, reserveSpec{
		Import: created, PrincipalID: request.PrincipalID, APIMajorVersion: apiMajorVersion,
		Operation: operationCreateImport, IdempotencyKey: request.IdempotencyKey,
		RequestFingerprint:   fingerprint,
		RequestExceedsLimit:  request.ExpectedSize != nil && *request.ExpectedSize > service.config.MaxUploadBytes,
		IdempotencyExpiresAt: now.Add(service.config.IdempotencyRetention),
	})
}
