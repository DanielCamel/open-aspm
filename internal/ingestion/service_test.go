package ingestion

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReserveAuthorizesAndBuildsDeterministicReservation(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	store := &recordingStore{}
	service := newTestService(t, store, authorizer)
	fixedTime := time.Date(2026, time.September, 23, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return fixedTime }
	service.random = func(buffer []byte) error {
		for index := range buffer {
			buffer[index] = byte(index + 1)
		}
		return nil
	}
	size := int64(48127)
	request := ReserveRequest{
		PrincipalID: "principal-a", WorkspaceID: "workspace-a", ApplicationID: "application-a",
		IdempotencyKey: "request-1", ReportFormat: ReportFormat{Name: "sarif", Version: "2.1.0"},
		OriginalFilename: "results.sarif", ExpectedSize: &size,
		ExpectedSHA256: "4f7f2e849fcb4517f07bca75f0cb56d042da07f86f9f86c34d828b8e25f0107a",
	}

	result, err := service.Reserve(context.Background(), request)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if len(authorizer.requests) != 1 || authorizer.requests[0] != (AuthorizationRequest{
		PrincipalID: "principal-a", WorkspaceID: "workspace-a",
		ApplicationID: "application-a", Capability: CapabilityImportsCreate,
	}) {
		t.Fatalf("authorization requests = %+v", authorizer.requests)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want one", store.calls)
	}
	if result.Import.ID != "imp_aebagbafaydqqcikbmga2dqpcaireeyu" {
		t.Fatalf("import ID = %q", result.Import.ID)
	}
	if result.Import.State != ImportAwaitingUpload || result.Import.MaxBytes != 100<<20 {
		t.Fatalf("import = %+v", result.Import)
	}
	if !result.Import.UploadExpiresAt.Equal(fixedTime.Add(30 * time.Minute)) {
		t.Fatalf("upload expiry = %s", result.Import.UploadExpiresAt)
	}
	if store.spec.APIMajorVersion != 1 || store.spec.Operation != "imports.create" ||
		store.spec.IdempotencyKey != request.IdempotencyKey ||
		!store.spec.IdempotencyExpiresAt.Equal(fixedTime.Add(24*time.Hour)) {
		t.Fatalf("store spec = %+v", store.spec)
	}
	if result.Import.ExpectedSize == request.ExpectedSize {
		t.Fatal("expected-size pointer was retained from caller input")
	}
}

func TestReserveDenialPerformsNoStoreWrite(t *testing.T) {
	authorizer := &recordingAuthorizer{err: ErrForbidden}
	store := &recordingStore{}
	service := newTestService(t, store, authorizer)

	_, err := service.Reserve(context.Background(), validRequest())
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("Reserve() error = %v, want ErrForbidden", err)
	}
	if store.calls != 0 {
		t.Fatalf("store calls = %d, want zero", store.calls)
	}
}

func TestReserveValidatesContractFields(t *testing.T) {
	tooLarge := int64(101 << 20)
	negative := int64(-1)
	tests := []struct {
		name           string
		mutate         func(*ReserveRequest)
		wantErr        error
		wantStoreCalls int
	}{
		{name: "principal", mutate: func(request *ReserveRequest) { request.PrincipalID = " x" }, wantErr: ErrInvalid},
		{name: "idempotency key", mutate: func(request *ReserveRequest) { request.IdempotencyKey = "bad key" }, wantErr: ErrInvalid},
		{name: "format", mutate: func(request *ReserveRequest) { request.ReportFormat.Name = "cyclonedx" }, wantErr: ErrInvalid},
		{name: "format version", mutate: func(request *ReserveRequest) { request.ReportFormat.Version = "2.0.0" }, wantErr: ErrInvalid},
		{name: "filename", mutate: func(request *ReserveRequest) { request.OriginalFilename = strings.Repeat("a", 256) }, wantErr: ErrInvalid},
		{name: "negative size", mutate: func(request *ReserveRequest) { request.ExpectedSize = &negative }, wantErr: ErrInvalid},
		{name: "oversize", mutate: func(request *ReserveRequest) { request.ExpectedSize = &tooLarge }, wantErr: ErrTooLarge, wantStoreCalls: 1},
		{name: "uppercase digest", mutate: func(request *ReserveRequest) {
			request.ExpectedSHA256 = "4F7F2E849FCB4517F07BCA75F0CB56D042DA07F86F9F86C34D828B8E25F0107A"
		}, wantErr: ErrInvalid},
		{name: "short digest", mutate: func(request *ReserveRequest) { request.ExpectedSHA256 = "abcd" }, wantErr: ErrInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authorizer := &recordingAuthorizer{}
			store := &recordingStore{}
			service := newTestService(t, store, authorizer)
			request := validRequest()
			test.mutate(&request)

			_, err := service.Reserve(context.Background(), request)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Reserve() error = %v, want %v", err, test.wantErr)
			}
			if store.calls != test.wantStoreCalls {
				t.Fatalf("store calls = %d, want %d", store.calls, test.wantStoreCalls)
			}
		})
	}
}

func TestRequestFingerprintUsesValidatedSemantics(t *testing.T) {
	request := validRequest()
	first, err := fingerprintRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fingerprintRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("identical validated request fields changed the request fingerprint")
	}
	changed := request
	changed.OriginalFilename = "other.sarif"
	third, err := fingerprintRequest(changed)
	if err != nil {
		t.Fatal(err)
	}
	if first == third {
		t.Fatal("different validated request fields produced the same fingerprint")
	}
}

func TestNewServiceRejectsUnsafeConfiguration(t *testing.T) {
	valid := Config{
		MaxUploadBytes: 100 << 20, UploadReservationTTL: 30 * time.Minute,
		IdempotencyRetention: 24 * time.Hour,
	}
	tests := []struct {
		name   string
		config Config
	}{
		{name: "zero max bytes", config: Config{UploadReservationTTL: valid.UploadReservationTTL, IdempotencyRetention: valid.IdempotencyRetention}},
		{name: "zero upload ttl", config: Config{MaxUploadBytes: valid.MaxUploadBytes, IdempotencyRetention: valid.IdempotencyRetention}},
		{name: "short idempotency retention", config: Config{MaxUploadBytes: valid.MaxUploadBytes, UploadReservationTTL: valid.UploadReservationTTL, IdempotencyRetention: 23 * time.Hour}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewService(&recordingStore{}, &recordingAuthorizer{}, test.config); !errors.Is(err, ErrInvalid) {
				t.Fatalf("NewService() error = %v, want ErrInvalid", err)
			}
		})
	}
}

type recordingAuthorizer struct {
	requests []AuthorizationRequest
	err      error
}

func (authorizer *recordingAuthorizer) Authorize(_ context.Context, request AuthorizationRequest) error {
	authorizer.requests = append(authorizer.requests, request)
	return authorizer.err
}

type recordingStore struct {
	calls int
	spec  reserveSpec
}

func (store *recordingStore) Reserve(_ context.Context, spec reserveSpec) (Reservation, error) {
	store.calls++
	store.spec = spec
	if spec.RequestExceedsLimit {
		return Reservation{}, ErrTooLarge
	}
	return Reservation{Import: spec.Import, Created: true}, nil
}

func newTestService(t *testing.T, store reservationStore, authorizer Authorizer) *Service {
	t.Helper()
	service, err := NewService(store, authorizer, Config{
		MaxUploadBytes: 100 << 20, UploadReservationTTL: 30 * time.Minute,
		IdempotencyRetention: 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func validRequest() ReserveRequest {
	return ReserveRequest{
		PrincipalID: "principal-a", WorkspaceID: "workspace-a", ApplicationID: "application-a",
		IdempotencyKey: "request-1", ReportFormat: ReportFormat{Name: "sarif", Version: "2.1.0"},
	}
}
