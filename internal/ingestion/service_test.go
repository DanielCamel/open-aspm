package ingestion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/spectremi/open-aspm/internal/blobstore"
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
	if store.spec.ArtifactID != "art_aebagbafaydqqcikbmga2dqpcaireeyu" ||
		store.spec.StorageBackend != "test" {
		t.Fatalf("raw artifact reservation = %+v", store.spec)
	}
	if _, err := blobstore.ParseKey(store.spec.StorageKey); err != nil {
		t.Fatalf("storage key = %q: %v", store.spec.StorageKey, err)
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
		UploadTimeout: time.Minute, IdempotencyRetention: 24 * time.Hour,
		StorageBackend: "test",
	}
	tests := []struct {
		name   string
		config Config
	}{
		{name: "zero max bytes", config: Config{UploadReservationTTL: valid.UploadReservationTTL, IdempotencyRetention: valid.IdempotencyRetention}},
		{name: "zero upload ttl", config: Config{MaxUploadBytes: valid.MaxUploadBytes, UploadTimeout: valid.UploadTimeout, IdempotencyRetention: valid.IdempotencyRetention, StorageBackend: valid.StorageBackend}},
		{name: "zero upload timeout", config: Config{MaxUploadBytes: valid.MaxUploadBytes, UploadReservationTTL: valid.UploadReservationTTL, IdempotencyRetention: valid.IdempotencyRetention, StorageBackend: valid.StorageBackend}},
		{name: "short idempotency retention", config: Config{MaxUploadBytes: valid.MaxUploadBytes, UploadReservationTTL: valid.UploadReservationTTL, UploadTimeout: valid.UploadTimeout, IdempotencyRetention: 23 * time.Hour, StorageBackend: valid.StorageBackend}},
		{name: "bad storage backend", config: Config{MaxUploadBytes: valid.MaxUploadBytes, UploadReservationTTL: valid.UploadReservationTTL, UploadTimeout: valid.UploadTimeout, IdempotencyRetention: valid.IdempotencyRetention, StorageBackend: "Bad backend"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewService(&recordingStore{}, &recordingBlobStore{}, &recordingAuthorizer{}, test.config); !errors.Is(err, ErrInvalid) {
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
	calls        int
	spec         reserveSpec
	beginCalls   int
	beginSpec    beginUploadSpec
	beginResult  uploadSession
	beginErr     error
	commitCalls  int
	commitSpec   commitUploadSpec
	commitResult UploadReceipt
	commitErr    error
	endSpecs     []endUploadSpec
	endErr       error
}

func TestUploadAuthorizesStreamsAndCommitsVerifiedMetadata(t *testing.T) {
	content := []byte("synthetic sarif evidence")
	digest := sha256.Sum256(content)
	digestHex := hex.EncodeToString(digest[:])
	key := mustBlobKey(t)
	committedAt := time.Date(2026, time.September, 23, 12, 1, 0, 0, time.UTC)
	store := &recordingStore{
		beginResult: uploadSession{
			WorkspaceID: "workspace-a", ImportID: "import-a", ArtifactID: "artifact-a",
			StorageBackend: "test", StorageKey: key.String(), AttemptID: "attempt-a", MaxBytes: 1024,
		},
		commitResult: UploadReceipt{
			ImportID: "import-a", State: ImportUploaded, SizeBytes: int64(len(content)),
			SHA256: digestHex, UploadedAt: committedAt,
		},
	}
	blobs := &recordingBlobStore{
		statErr:  blobstore.ErrNotFound,
		metadata: blobstore.Metadata{Version: "version-a", BackendVersion: "backend-a", CommittedAt: committedAt},
	}
	authorizer := &recordingAuthorizer{}
	service := newUploadTestService(t, store, blobs, authorizer, key)
	declared := int64(len(content))

	receipt, err := service.Upload(context.Background(), UploadRequest{
		PrincipalID: "principal-a", WorkspaceID: "workspace-a", ApplicationID: "application-a",
		ImportID: "import-a", DeclaredSize: &declared, Content: bytes.NewReader(content),
	})
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if receipt != store.commitResult {
		t.Fatalf("receipt = %+v, want %+v", receipt, store.commitResult)
	}
	if len(authorizer.requests) != 1 || authorizer.requests[0].Capability != CapabilityImportsUpload {
		t.Fatalf("authorization requests = %+v", authorizer.requests)
	}
	if store.beginCalls != 1 || store.commitCalls != 1 || blobs.putCalls != 1 {
		t.Fatalf("calls = begin %d, put %d, commit %d", store.beginCalls, blobs.putCalls, store.commitCalls)
	}
	if store.commitSpec.SizeBytes != int64(len(content)) || store.commitSpec.SHA256 != digestHex ||
		store.commitSpec.StorageVersion != "version-a" || store.commitSpec.BackendVersion != "backend-a" {
		t.Fatalf("commit spec = %+v", store.commitSpec)
	}
	if len(store.endSpecs) != 0 {
		t.Fatalf("successful upload ended attempt unexpectedly: %+v", store.endSpecs)
	}
}

func TestUploadDenialPerformsNoPersistenceOrBlobIO(t *testing.T) {
	store := &recordingStore{}
	blobs := &recordingBlobStore{}
	service := newUploadTestService(t, store, blobs, &recordingAuthorizer{err: ErrForbidden}, mustBlobKey(t))

	_, err := service.Upload(context.Background(), validUploadRequest("evidence"))
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("Upload() error = %v, want ErrForbidden", err)
	}
	if store.beginCalls != 0 || store.commitCalls != 0 || blobs.statCalls != 0 || blobs.putCalls != 0 {
		t.Fatalf("denied upload touched state: store=%+v blobs=%+v", store, blobs)
	}
}

func TestUploadExpectationMismatchRejectsWithoutPublishingMetadata(t *testing.T) {
	key := mustBlobKey(t)
	store := &recordingStore{beginResult: uploadSession{
		WorkspaceID: "workspace-a", ImportID: "import-a", ArtifactID: "artifact-a",
		StorageBackend: "test", StorageKey: key.String(), AttemptID: "attempt-a", MaxBytes: 1024,
		ExpectedSHA256: strings.Repeat("0", 64),
	}}
	blobs := &recordingBlobStore{statErr: blobstore.ErrNotFound}
	service := newUploadTestService(t, store, blobs, &recordingAuthorizer{}, key)

	_, err := service.Upload(context.Background(), validUploadRequest("different evidence"))
	if !errors.Is(err, ErrUploadMismatch) {
		t.Fatalf("Upload() error = %v, want ErrUploadMismatch", err)
	}
	if store.commitCalls != 0 || len(store.endSpecs) != 1 || store.endSpecs[0].RejectCode != "content_mismatch" {
		t.Fatalf("upload state calls = commit %d, end %+v", store.commitCalls, store.endSpecs)
	}
}

func TestUploadReplayRequiresIdenticalContent(t *testing.T) {
	content := []byte("committed evidence")
	digest := sha256.Sum256(content)
	store := &recordingStore{beginResult: uploadSession{
		WorkspaceID: "workspace-a", ImportID: "import-a", ArtifactID: "artifact-a",
		MaxBytes: 1024, Committed: true,
		Receipt: UploadReceipt{
			ImportID: "import-a", State: ImportUploaded, SizeBytes: int64(len(content)),
			SHA256: hex.EncodeToString(digest[:]), UploadedAt: time.Date(2026, 9, 23, 12, 1, 0, 0, time.UTC),
		},
	}}
	blobs := &recordingBlobStore{}
	service := newUploadTestService(t, store, blobs, &recordingAuthorizer{}, mustBlobKey(t))

	request := validUploadRequest(string(content))
	receipt, err := service.Upload(context.Background(), request)
	if err != nil || receipt != store.beginResult.Receipt {
		t.Fatalf("identical replay = (%+v, %v)", receipt, err)
	}
	conflict := validUploadRequest("conflicting evidence")
	if _, err := service.Upload(context.Background(), conflict); !errors.Is(err, ErrUploadConflict) {
		t.Fatalf("conflicting replay error = %v, want ErrUploadConflict", err)
	}
	if blobs.statCalls != 0 || blobs.putCalls != 0 || store.commitCalls != 0 {
		t.Fatalf("replay touched blob or commit state: blobs=%+v commits=%d", blobs, store.commitCalls)
	}
}

func TestUploadRecoversCommittedBlobAfterMetadataFailure(t *testing.T) {
	content := []byte("recoverable evidence")
	digest := sha256.Sum256(content)
	key := mustBlobKey(t)
	committedAt := time.Date(2026, 9, 23, 12, 1, 0, 0, time.UTC)
	metadata := blobstore.Metadata{
		Key: key, Size: int64(len(content)), SHA256: hex.EncodeToString(digest[:]),
		Version: "version-a", CommittedAt: committedAt,
	}
	store := &recordingStore{
		beginResult: uploadSession{
			WorkspaceID: "workspace-a", ImportID: "import-a", ArtifactID: "artifact-a",
			StorageBackend: "test", StorageKey: key.String(), AttemptID: "attempt-b", MaxBytes: 1024,
		},
		commitResult: UploadReceipt{
			ImportID: "import-a", State: ImportUploaded, SizeBytes: int64(len(content)),
			SHA256: metadata.SHA256, UploadedAt: committedAt,
		},
	}
	blobs := &recordingBlobStore{statMetadata: metadata}
	service := newUploadTestService(t, store, blobs, &recordingAuthorizer{}, key)

	receipt, err := service.Upload(context.Background(), validUploadRequest(string(content)))
	if err != nil || receipt != store.commitResult {
		t.Fatalf("recovery Upload() = (%+v, %v)", receipt, err)
	}
	if blobs.putCalls != 0 || store.commitCalls != 1 {
		t.Fatalf("recovery calls = put %d, commit %d", blobs.putCalls, store.commitCalls)
	}
}

func TestUploadFailureClassificationAndAttemptCleanup(t *testing.T) {
	tests := []struct {
		name       string
		content    io.Reader
		declared   *int64
		expected   *int64
		blobErr    error
		wantErr    error
		wantReject string
	}{
		{
			name: "declared oversize", content: readerFunc(func([]byte) (int, error) {
				t.Fatal("oversized declared request body was read")
				return 0, io.EOF
			}),
			declared: int64Pointer(1025), wantErr: ErrTooLarge, wantReject: "size_limit_exceeded",
		},
		{
			name: "truncated expected content", content: strings.NewReader("short"),
			expected: int64Pointer(10), wantErr: ErrUploadMismatch, wantReject: "content_mismatch",
		},
		{
			name: "backend failure", content: strings.NewReader("evidence"),
			blobErr: blobstore.ErrBackend, wantErr: blobstore.ErrBackend,
		},
		{
			name: "interrupted stream", content: readerFunc(func([]byte) (int, error) {
				return 0, io.ErrUnexpectedEOF
			}),
			wantErr: io.ErrUnexpectedEOF,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := mustBlobKey(t)
			store := &recordingStore{beginResult: uploadSession{
				WorkspaceID: "workspace-a", ImportID: "import-a", ArtifactID: "artifact-a",
				StorageBackend: "test", StorageKey: key.String(), AttemptID: "attempt-a",
				MaxBytes: 1024, ExpectedSize: test.expected,
			}}
			blobs := &recordingBlobStore{statErr: blobstore.ErrNotFound, putErr: test.blobErr}
			service := newUploadTestService(t, store, blobs, &recordingAuthorizer{}, key)
			request := validUploadRequest("")
			request.Content = test.content
			request.DeclaredSize = test.declared

			_, err := service.Upload(context.Background(), request)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Upload() error = %v, want %v", err, test.wantErr)
			}
			if len(store.endSpecs) != 1 || store.endSpecs[0].RejectCode != test.wantReject {
				t.Fatalf("end specs = %+v, want rejection %q", store.endSpecs, test.wantReject)
			}
			if store.commitCalls != 0 {
				t.Fatalf("commit calls = %d, want zero", store.commitCalls)
			}
		})
	}
}

func (store *recordingStore) BeginUpload(_ context.Context, spec beginUploadSpec) (uploadSession, error) {
	store.beginCalls++
	store.beginSpec = spec
	return store.beginResult, store.beginErr
}

func (store *recordingStore) CommitUpload(_ context.Context, spec commitUploadSpec) (UploadReceipt, error) {
	store.commitCalls++
	store.commitSpec = spec
	return store.commitResult, store.commitErr
}

func (store *recordingStore) EndUpload(_ context.Context, spec endUploadSpec) error {
	store.endSpecs = append(store.endSpecs, spec)
	return store.endErr
}

type recordingBlobStore struct {
	putCalls     int
	putKey       blobstore.Key
	putErr       error
	metadata     blobstore.Metadata
	statCalls    int
	statErr      error
	statMetadata blobstore.Metadata
}

type readerFunc func([]byte) (int, error)

func (function readerFunc) Read(buffer []byte) (int, error) {
	return function(buffer)
}

func (store *recordingBlobStore) Put(
	ctx context.Context,
	key blobstore.Key,
	source io.Reader,
	options blobstore.PutOptions,
) (blobstore.Metadata, error) {
	store.putCalls++
	store.putKey = key
	if store.putErr != nil {
		return blobstore.Metadata{}, store.putErr
	}
	bounded := blobstore.NewBoundedReader(ctx, source, options.MaxBytes)
	if _, err := io.Copy(io.Discard, bounded); err != nil {
		return blobstore.Metadata{}, err
	}
	size, digest, err := bounded.Result()
	if err != nil {
		return blobstore.Metadata{}, err
	}
	if err := blobstore.VerifyExpected(size, digest, options); err != nil {
		return blobstore.Metadata{}, err
	}
	metadata := store.metadata
	metadata.Key = key
	metadata.Size = size
	metadata.SHA256 = digest
	if metadata.Version == "" {
		metadata.Version = "version-a"
	}
	if metadata.CommittedAt.IsZero() {
		metadata.CommittedAt = time.Date(2026, time.September, 23, 12, 1, 0, 0, time.UTC)
	}
	return metadata, nil
}

func (store *recordingBlobStore) Stat(context.Context, blobstore.Key) (blobstore.Metadata, error) {
	store.statCalls++
	return store.statMetadata, store.statErr
}

func (store *recordingStore) Reserve(_ context.Context, spec reserveSpec) (Reservation, error) {
	store.calls++
	store.spec = spec
	if spec.RequestExceedsLimit {
		return Reservation{}, ErrTooLarge
	}
	return Reservation{Import: spec.Import, Created: true}, nil
}

func newTestService(t *testing.T, store ingestionStore, authorizer Authorizer) *Service {
	t.Helper()
	service, err := NewService(store, &recordingBlobStore{statErr: blobstore.ErrNotFound}, authorizer, Config{
		MaxUploadBytes: 100 << 20, UploadReservationTTL: 30 * time.Minute,
		UploadTimeout: time.Minute, IdempotencyRetention: 24 * time.Hour,
		StorageBackend: "test",
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

func validUploadRequest(content string) UploadRequest {
	return UploadRequest{
		PrincipalID: "principal-a", WorkspaceID: "workspace-a", ApplicationID: "application-a",
		ImportID: "import-a", Content: strings.NewReader(content),
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}

func mustBlobKey(t *testing.T) blobstore.Key {
	t.Helper()
	key, err := blobstore.ParseKey("blb_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func newUploadTestService(
	t *testing.T,
	store ingestionStore,
	blobs blobWriter,
	authorizer Authorizer,
	key blobstore.Key,
) *Service {
	t.Helper()
	service, err := NewService(store, blobs, authorizer, Config{
		MaxUploadBytes: 100 << 20, UploadReservationTTL: 30 * time.Minute,
		UploadTimeout: time.Minute, IdempotencyRetention: 24 * time.Hour,
		StorageBackend: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC) }
	service.random = func(buffer []byte) error {
		for index := range buffer {
			buffer[index] = byte(index + 1)
		}
		return nil
	}
	service.newBlobKey = func() (blobstore.Key, error) { return key, nil }
	return service
}
