package ingestion

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var processingFailureCodePattern = regexp.MustCompile(`^[a-z][a-z0-9.-]{0,127}$`)

var (
	ErrProcessingConflict  = errors.New("import processing state conflicts with requested transition")
	ErrProcessingNotFound  = errors.New("import processing target not found")
	ErrParseOutputConflict = errors.New("parser output conflicts with existing immutable output")
)

// ProcessingIdentity binds an internal job to its public operation and import.
type ProcessingIdentity struct {
	WorkspaceID   string
	OperationID   string
	ImportID      string
	PrincipalID   string
	JobCapability string
}

// ProcessingTarget contains only the resource scope needed for authorization.
// Evidence storage metadata is returned only by BeginProcessing after the
// application service has authorized this target.
type ProcessingTarget struct {
	WorkspaceID   string
	OperationID   string
	ImportID      string
	ApplicationID string
	State         OperationState
	FailureCode   string
}

// ProcessingSource is the authorized immutable input for one import job.
type ProcessingSource struct {
	ProcessingTarget
	RawArtifactID  string
	ReportFormat   ReportFormat
	StorageBackend string
	StorageKey     string
	StorageVersion string
	BackendVersion string
	SizeBytes      int64
	SHA256         string
	MaxBytes       int64
	ReceivedAt     time.Time
}

// ProcessingResult is the public operation result defined by ingestion-v1.
type ProcessingResult struct {
	ScansCreated int
}

// ProcessingFailure is bounded and safe for the public operation resource.
type ProcessingFailure struct {
	Code   string
	Title  string
	Detail string
}

// ParseWarning preserves a parser-generated location and unsupported source
// field names without copying arbitrary report values into diagnostics.
type ParseWarning struct {
	Code            string   `json:"code"`
	SourcePointer   string   `json:"source_pointer"`
	FieldCount      int      `json:"field_count"`
	Fields          []string `json:"fields"`
	FieldsTruncated bool     `json:"fields_truncated"`
}

// ParseOutputSpec identifies one deterministic parser interpretation of an
// immutable raw artifact.
type ParseOutputSpec struct {
	WorkspaceID       string
	ImportID          string
	RawArtifactID     string
	FormatName        string
	FormatVersion     string
	ParserName        string
	ParserVersion     string
	Warnings          []ParseWarning
	WarningsTruncated bool
	RecordedAt        time.Time
}

func validateProcessingIdentity(identity ProcessingIdentity) error {
	if !validOpaqueID(identity.WorkspaceID) || !validOpaqueID(identity.OperationID) ||
		!validOpaqueID(identity.ImportID) || !validOpaqueID(identity.PrincipalID) ||
		identity.JobCapability != CapabilityImportsProcess {
		return ErrInvalid
	}
	return nil
}

func canonicalizeParseOutput(spec ParseOutputSpec) ([]ParseWarning, [sha256.Size]byte, error) {
	if !validOpaqueID(spec.WorkspaceID) || !validOpaqueID(spec.ImportID) ||
		!validOpaqueID(spec.RawArtifactID) || !validText(spec.FormatName, 1, 32) ||
		!validText(spec.FormatVersion, 1, 32) || !domainNamePattern.MatchString(spec.ParserName) ||
		!validText(spec.ParserVersion, 1, 64) || spec.RecordedAt.IsZero() || len(spec.Warnings) > 1000 {
		return nil, [sha256.Size]byte{}, ErrInvalid
	}
	warnings := make([]ParseWarning, len(spec.Warnings))
	for index, warning := range spec.Warnings {
		if !domainNamePattern.MatchString(warning.Code) || len(warning.Code) > 64 ||
			!validText(warning.SourcePointer, 0, 1024) ||
			warning.FieldCount <= 0 || len(warning.Fields) > 64 || len(warning.Fields) > warning.FieldCount {
			return nil, [sha256.Size]byte{}, ErrInvalid
		}
		fields := make([]string, len(warning.Fields))
		copy(fields, warning.Fields)
		for _, field := range fields {
			if field == "" || !utf8.ValidString(field) || len(field) > 1<<20 || strings.ContainsRune(field, 0) {
				return nil, [sha256.Size]byte{}, ErrInvalid
			}
		}
		warnings[index] = warning
		warnings[index].Fields = fields
	}
	input := struct {
		WorkspaceID       string         `json:"workspace_id"`
		ImportID          string         `json:"import_id"`
		RawArtifactID     string         `json:"raw_artifact_id"`
		FormatName        string         `json:"format_name"`
		FormatVersion     string         `json:"format_version"`
		ParserName        string         `json:"parser_name"`
		ParserVersion     string         `json:"parser_version"`
		Warnings          []ParseWarning `json:"warnings"`
		WarningsTruncated bool           `json:"warnings_truncated"`
	}{
		WorkspaceID: spec.WorkspaceID, ImportID: spec.ImportID, RawArtifactID: spec.RawArtifactID,
		FormatName: spec.FormatName, FormatVersion: spec.FormatVersion,
		ParserName: spec.ParserName, ParserVersion: spec.ParserVersion,
		Warnings: warnings, WarningsTruncated: spec.WarningsTruncated,
	}
	encoded, _ := json.Marshal(input)
	return warnings, sha256.Sum256(encoded), nil
}

func validFailure(failure ProcessingFailure) bool {
	return processingFailureCodePattern.MatchString(failure.Code) && validText(failure.Title, 1, 255) &&
		(failure.Detail == "" || (len(failure.Detail) <= 2048 && !strings.ContainsRune(failure.Detail, 0)))
}
