// Package ingestion owns import reservation and raw-artifact ingestion state.
package ingestion

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	CapabilityImportsCreate = "imports:create"
	apiMajorVersion         = 1
	operationCreateImport   = "imports.create"
	minimumRetention        = 24 * time.Hour
)

var (
	ErrApplicationNotFound   = errors.New("application not found")
	ErrForbidden             = errors.New("ingestion operation forbidden")
	ErrIdempotencyConflict   = errors.New("idempotency key reused for a different request")
	ErrIdempotencyInProgress = errors.New("idempotent request is already in progress")
	ErrInvalid               = errors.New("invalid ingestion input")
	ErrTooLarge              = errors.New("report exceeds the import limit")
)

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// ImportState is the public import lifecycle, independent from internal job
// lease state.
type ImportState string

const (
	ImportAwaitingUpload ImportState = "awaiting_upload"
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

type reserveSpec struct {
	Import               Import
	PrincipalID          string
	APIMajorVersion      int
	Operation            string
	IdempotencyKey       string
	RequestFingerprint   [sha256.Size]byte
	RequestExceedsLimit  bool
	IdempotencyExpiresAt time.Time
}

func validateAuthorizationScope(request ReserveRequest) error {
	if !validOpaqueID(request.PrincipalID) || !validOpaqueID(request.WorkspaceID) ||
		!validOpaqueID(request.ApplicationID) || !idempotencyKeyPattern.MatchString(request.IdempotencyKey) {
		return ErrInvalid
	}
	return nil
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
