// Package ingestion owns import reservation and raw-artifact ingestion state.
package ingestion

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/spectremi/open-aspm/internal/blobstore"
)

const (
	CapabilityImportsCreate = "imports:create"
	CapabilityImportsUpload = "imports:upload"
	apiMajorVersion         = 1
	operationCreateImport   = "imports.create"
	minimumRetention        = 24 * time.Hour
	rawArtifactMediaType    = "application/octet-stream"
)

var (
	ErrApplicationNotFound   = errors.New("application not found")
	ErrForbidden             = errors.New("ingestion operation forbidden")
	ErrIdempotencyConflict   = errors.New("idempotency key reused for a different request")
	ErrIdempotencyInProgress = errors.New("idempotent request is already in progress")
	ErrImportNotFound        = errors.New("import not found")
	ErrInvalid               = errors.New("invalid ingestion input")
	ErrTooLarge              = errors.New("report exceeds the import limit")
	ErrUploadConflict        = errors.New("uploaded content conflicts with committed evidence")
	ErrUploadExpired         = errors.New("upload reservation expired")
	ErrUploadInProgress      = errors.New("another upload attempt is in progress")
	ErrUploadLeaseLost       = errors.New("upload attempt lease was lost")
	ErrUploadMismatch        = errors.New("uploaded content does not match declared expectations")
	ErrUploadStorage         = errors.New("raw artifact storage backend is unavailable")
)

var (
	idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	storageBackendPattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
)

// ImportState is the public import lifecycle, independent from internal job
// lease state.
type ImportState string

const (
	ImportAwaitingUpload ImportState = "awaiting_upload"
	ImportUploading      ImportState = "uploading"
	ImportUploaded       ImportState = "uploaded"
	ImportRejected       ImportState = "rejected"
	ImportAbandoned      ImportState = "abandoned"
)

type rawArtifactState string

const (
	rawArtifactPending   rawArtifactState = "pending"
	rawArtifactUploading rawArtifactState = "uploading"
	rawArtifactCommitted rawArtifactState = "committed"
	rawArtifactRejected  rawArtifactState = "rejected"
)

// ReportFormat identifies the declared report representation. It does not
// allow the client to choose parser implementation or behavior versions.
type ReportFormat struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// ReserveRequest is the validated semantic input to createImport. Empty
// optional strings mean that the corresponding API field was omitted.
type ReserveRequest struct {
	PrincipalID      string
	WorkspaceID      string
	ApplicationID    string
	IdempotencyKey   string
	ReportFormat     ReportFormat
	OriginalFilename string
	ExpectedSize     *int64
	ExpectedSHA256   string
}

// Import is the durable reservation returned by the application service.
type Import struct {
	ID                   string
	WorkspaceID          string
	ApplicationID        string
	CreatedByPrincipalID string
	State                ImportState
	ReportFormat         ReportFormat
	OriginalFilename     string
	ExpectedSize         *int64
	ExpectedSHA256       string
	MaxBytes             int64
	UploadExpiresAt      time.Time
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// Reservation reports whether this call created the import. Replays return
// the original Import with Created false; an HTTP adapter must still replay
// the original 201 response defined by the v1 contract.
type Reservation struct {
	Import  Import
	Created bool
}

// UploadRequest is the trusted application-service input for the reserved
// content resource. ApplicationID is resolved from authorized routing context;
// it is not accepted from the public upload body.
type UploadRequest struct {
	PrincipalID   string
	WorkspaceID   string
	ApplicationID string
	ImportID      string
	DeclaredSize  *int64
	Content       io.Reader
}

// UploadReceipt contains public evidence metadata. Storage references remain
// internal and are never returned to clients.
type UploadReceipt struct {
	ImportID   string
	State      ImportState
	SizeBytes  int64
	SHA256     string
	UploadedAt time.Time
}

type reserveSpec struct {
	Import               Import
	ArtifactID           string
	StorageBackend       string
	StorageKey           string
	PrincipalID          string
	APIMajorVersion      int
	Operation            string
	IdempotencyKey       string
	RequestFingerprint   [sha256.Size]byte
	RequestExceedsLimit  bool
	IdempotencyExpiresAt time.Time
}

type beginUploadSpec struct {
	PrincipalID    string
	WorkspaceID    string
	ApplicationID  string
	ImportID       string
	ArtifactID     string
	StorageBackend string
	StorageKey     string
	AttemptID      string
	StartedAt      time.Time
	LeaseExpiresAt time.Time
}

type uploadSession struct {
	WorkspaceID    string
	ImportID       string
	ArtifactID     string
	StorageBackend string
	StorageKey     string
	AttemptID      string
	MaxBytes       int64
	ExpectedSize   *int64
	ExpectedSHA256 string
	Committed      bool
	Receipt        UploadReceipt
}

type commitUploadSpec struct {
	WorkspaceID    string
	ImportID       string
	ArtifactID     string
	AttemptID      string
	SizeBytes      int64
	SHA256         string
	StorageVersion string
	BackendVersion string
	CommittedAt    time.Time
}

type endUploadSpec struct {
	WorkspaceID string
	ImportID    string
	ArtifactID  string
	AttemptID   string
	EndedAt     time.Time
	RejectCode  string
}

func validateAuthorizationScope(request ReserveRequest) error {
	if !validOpaqueID(request.PrincipalID) || !validOpaqueID(request.WorkspaceID) ||
		!validOpaqueID(request.ApplicationID) || !idempotencyKeyPattern.MatchString(request.IdempotencyKey) {
		return ErrInvalid
	}
	return nil
}

func validateUploadRequest(request UploadRequest) error {
	if !validOpaqueID(request.PrincipalID) || !validOpaqueID(request.WorkspaceID) ||
		!validOpaqueID(request.ApplicationID) || !validOpaqueID(request.ImportID) ||
		request.Content == nil {
		return ErrInvalid
	}
	if request.DeclaredSize != nil && *request.DeclaredSize < 0 {
		return ErrInvalid
	}
	return nil
}

func parseStorageKey(value string) (blobstore.Key, error) {
	key, err := blobstore.ParseKey(value)
	if err != nil {
		return blobstore.Key{}, fmt.Errorf("restore raw artifact storage key: %w", err)
	}
	return key, nil
}

func fingerprintRequest(request ReserveRequest) ([sha256.Size]byte, error) {
	if request.ReportFormat.Name != "sarif" ||
		(request.ReportFormat.Version != "" && request.ReportFormat.Version != "2.1.0") {
		return [sha256.Size]byte{}, ErrInvalid
	}
	if request.OriginalFilename != "" &&
		(!utf8.ValidString(request.OriginalFilename) || strings.IndexByte(request.OriginalFilename, 0) >= 0 ||
			utf8.RuneCountInString(request.OriginalFilename) > 255) {
		return [sha256.Size]byte{}, ErrInvalid
	}
	if request.ExpectedSize != nil {
		if *request.ExpectedSize < 0 {
			return [sha256.Size]byte{}, ErrInvalid
		}
	}
	if request.ExpectedSHA256 != "" {
		decoded, err := hex.DecodeString(request.ExpectedSHA256)
		if err != nil || len(decoded) != sha256.Size || strings.ToLower(request.ExpectedSHA256) != request.ExpectedSHA256 {
			return [sha256.Size]byte{}, ErrInvalid
		}
	}

	input := struct {
		ApplicationID    string       `json:"application_id"`
		ReportFormat     ReportFormat `json:"report_format"`
		OriginalFilename string       `json:"original_filename,omitempty"`
		ExpectedSize     *int64       `json:"expected_size_bytes,omitempty"`
		ExpectedSHA256   string       `json:"expected_sha256,omitempty"`
	}{
		ApplicationID: request.ApplicationID, ReportFormat: request.ReportFormat,
		OriginalFilename: request.OriginalFilename, ExpectedSize: request.ExpectedSize,
		ExpectedSHA256: request.ExpectedSHA256,
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("fingerprint import reservation: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func validOpaqueID(value string) bool {
	length := utf8.RuneCountInString(value)
	return utf8.ValidString(value) && length >= 3 && length <= 128 &&
		strings.TrimSpace(value) == value && strings.IndexByte(value, 0) < 0
}

func encodeRandomID(prefix string, random []byte) string {
	return prefix + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(random))
}

func idempotencyLockKey(spec reserveSpec) string {
	input := struct {
		WorkspaceID     string `json:"workspace_id"`
		PrincipalID     string `json:"principal_id"`
		APIMajorVersion int    `json:"api_major_version"`
		Operation       string `json:"operation"`
		IdempotencyKey  string `json:"idempotency_key"`
	}{
		WorkspaceID: spec.Import.WorkspaceID, PrincipalID: spec.PrincipalID,
		APIMajorVersion: spec.APIMajorVersion, Operation: spec.Operation,
		IdempotencyKey: spec.IdempotencyKey,
	}
	encoded, _ := json.Marshal(input)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
