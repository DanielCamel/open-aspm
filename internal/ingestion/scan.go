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

var (
	ErrScanConflict       = errors.New("scan source record conflicts with existing immutable scan")
	ErrScanSourceNotFound = errors.New("scan import or committed evidence not found")
)

var domainNamePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,127}$`)

// ScanResult is the scanner-reported execution outcome. Parser success does
// not imply ScanSucceeded.
type ScanResult string

const (
	ScanResultSucceeded ScanResult = "succeeded"
	ScanResultFailed    ScanResult = "failed"
	ScanResultCancelled ScanResult = "cancelled"
	ScanResultTimedOut  ScanResult = "timed_out"
	ScanResultUnknown   ScanResult = "unknown"
)

// ScanCompleteness states whether omission can carry coverage meaning. Unknown
// is the conservative default for the first SARIF processing path.
type ScanCompleteness string

const (
	ScanCompletenessFull        ScanCompleteness = "full"
	ScanCompletenessPartial     ScanCompleteness = "partial"
	ScanCompletenessIncremental ScanCompleteness = "incremental"
	ScanCompletenessUnknown     ScanCompleteness = "unknown"
)

// ScanScope is the first versioned normalized scope. Empty optional fields are
// unknown, not false. CoverageMetadata is a bounded JSON object retained for
// source dimensions that are not authoritative normalized identities yet.
type ScanScope struct {
	SchemaVersion            int
	ScannerFamily            string
	ScannerInstanceID        string
	ScannerConfigurationID   string
	ScannerConfigurationHash string
	AnalysisKind             string
	CoverageMetadata         json.RawMessage
}

// ScanSpec identifies one source run in one immutable raw artifact.
type ScanSpec struct {
	ID                        string
	WorkspaceID               string
	ImportID                  string
	ApplicationID             string
	RawArtifactID             string
	SourceRunIndex            int
	Result                    ScanResult
	Completeness              ScanCompleteness
	ScannerName               string
	ScannerFullName           string
	ScannerVersion            string
	ScannerSemanticVersion    string
	AutomationID              string
	AutomationGUID            string
	AutomationCorrelationGUID string
	ParserName                string
	ParserVersion             string
	SourcePointer             string
	SourceStartedAt           *time.Time
	SourceEndedAt             *time.Time
	RecordedAt                time.Time
	Scope                     ScanScope
}

// StoredScan reports the durable scan identity. Replays return the originally
// stored ID and Created=false even when the caller generated another ID.
type StoredScan struct {
	ID            string
	WorkspaceID   string
	ImportID      string
	ApplicationID string
	RawArtifactID string
	ReceivedAt    time.Time
	RecordedAt    time.Time
	Created       bool
}

type canonicalScanSpec struct {
	WorkspaceID               string           `json:"workspace_id"`
	ImportID                  string           `json:"import_id"`
	ApplicationID             string           `json:"application_id"`
	RawArtifactID             string           `json:"raw_artifact_id"`
	SourceRunIndex            int              `json:"source_run_index"`
	Result                    ScanResult       `json:"result"`
	Completeness              ScanCompleteness `json:"completeness"`
	ScannerName               string           `json:"scanner_name"`
	ScannerFullName           string           `json:"scanner_full_name,omitempty"`
	ScannerVersion            string           `json:"scanner_version,omitempty"`
	ScannerSemanticVersion    string           `json:"scanner_semantic_version,omitempty"`
	AutomationID              string           `json:"automation_id,omitempty"`
	AutomationGUID            string           `json:"automation_guid,omitempty"`
	AutomationCorrelationGUID string           `json:"automation_correlation_guid,omitempty"`
	ParserName                string           `json:"parser_name"`
	ParserVersion             string           `json:"parser_version"`
	SourcePointer             string           `json:"source_pointer"`
	SourceStartedAt           *time.Time       `json:"source_started_at,omitempty"`
	SourceEndedAt             *time.Time       `json:"source_ended_at,omitempty"`
}

type canonicalScope struct {
	SchemaVersion            int             `json:"schema_version"`
	ApplicationID            string          `json:"application_id"`
	ScannerFamily            string          `json:"scanner_family"`
	ScannerInstanceID        string          `json:"scanner_instance_id,omitempty"`
	ScannerConfigurationID   string          `json:"scanner_configuration_id,omitempty"`
	ScannerConfigurationHash string          `json:"scanner_configuration_hash,omitempty"`
	AnalysisKind             string          `json:"analysis_kind"`
	CoverageMetadata         json.RawMessage `json:"coverage_metadata"`
}

func canonicalizeScanSpec(spec ScanSpec) (canonicalScanSpec, canonicalScope, [sha256.Size]byte, [sha256.Size]byte, error) {
	if !validOpaqueID(spec.ID) || !validOpaqueID(spec.WorkspaceID) || !validOpaqueID(spec.ImportID) ||
		!validOpaqueID(spec.ApplicationID) || !validOpaqueID(spec.RawArtifactID) || spec.SourceRunIndex < 0 ||
		!knownScanResult(spec.Result) || !knownScanCompleteness(spec.Completeness) || spec.RecordedAt.IsZero() {
		return canonicalScanSpec{}, canonicalScope{}, [sha256.Size]byte{}, [sha256.Size]byte{}, ErrInvalid
	}
	if !validText(spec.ScannerName, 1, 255) || !validOptionalText(spec.ScannerFullName, 512) ||
		!validOptionalText(spec.ScannerVersion, 128) || !validOptionalText(spec.ScannerSemanticVersion, 128) ||
		!validOptionalText(spec.AutomationID, 512) || !validOptionalText(spec.AutomationGUID, 128) ||
		!validOptionalText(spec.AutomationCorrelationGUID, 128) || !domainNamePattern.MatchString(spec.ParserName) ||
		!validText(spec.ParserVersion, 1, 64) || !validText(spec.SourcePointer, 1, 1024) {
		return canonicalScanSpec{}, canonicalScope{}, [sha256.Size]byte{}, [sha256.Size]byte{}, ErrInvalid
	}
	if spec.SourceStartedAt != nil && spec.SourceEndedAt != nil && spec.SourceEndedAt.Before(*spec.SourceStartedAt) {
		return canonicalScanSpec{}, canonicalScope{}, [sha256.Size]byte{}, [sha256.Size]byte{}, ErrInvalid
	}
	if spec.Scope.SchemaVersion <= 0 || !domainNamePattern.MatchString(spec.Scope.ScannerFamily) ||
		!validOptionalText(spec.Scope.ScannerInstanceID, 128) ||
		!validOptionalText(spec.Scope.ScannerConfigurationID, 128) ||
		!validOptionalText(spec.Scope.ScannerConfigurationHash, 128) ||
		!domainNamePattern.MatchString(spec.Scope.AnalysisKind) {
		return canonicalScanSpec{}, canonicalScope{}, [sha256.Size]byte{}, [sha256.Size]byte{}, ErrInvalid
	}
	coverage, err := canonicalJSONObject(spec.Scope.CoverageMetadata, 64<<10)
	if err != nil {
		return canonicalScanSpec{}, canonicalScope{}, [sha256.Size]byte{}, [sha256.Size]byte{}, ErrInvalid
	}
	scan := canonicalScanSpec{
		WorkspaceID: spec.WorkspaceID, ImportID: spec.ImportID, ApplicationID: spec.ApplicationID,
		RawArtifactID: spec.RawArtifactID, SourceRunIndex: spec.SourceRunIndex, Result: spec.Result,
		Completeness: spec.Completeness, ScannerName: spec.ScannerName, ScannerFullName: spec.ScannerFullName,
		ScannerVersion: spec.ScannerVersion, ScannerSemanticVersion: spec.ScannerSemanticVersion,
		AutomationID: spec.AutomationID, AutomationGUID: spec.AutomationGUID,
		AutomationCorrelationGUID: spec.AutomationCorrelationGUID, ParserName: spec.ParserName,
		ParserVersion: spec.ParserVersion, SourcePointer: spec.SourcePointer,
		SourceStartedAt: normalizeOptionalTime(spec.SourceStartedAt), SourceEndedAt: normalizeOptionalTime(spec.SourceEndedAt),
	}
	scope := canonicalScope{
		SchemaVersion: spec.Scope.SchemaVersion, ApplicationID: spec.ApplicationID,
		ScannerFamily: spec.Scope.ScannerFamily, ScannerInstanceID: spec.Scope.ScannerInstanceID,
		ScannerConfigurationID:   spec.Scope.ScannerConfigurationID,
		ScannerConfigurationHash: spec.Scope.ScannerConfigurationHash,
		AnalysisKind:             spec.Scope.AnalysisKind, CoverageMetadata: coverage,
	}
	scanJSON, _ := json.Marshal(scan)
	scopeJSON, _ := json.Marshal(scope)
	return scan, scope, sha256.Sum256(scanJSON), sha256.Sum256(scopeJSON), nil
}

func canonicalJSONObject(value json.RawMessage, maxBytes int) (json.RawMessage, error) {
	if len(value) == 0 {
		return json.RawMessage("{}"), nil
	}
	if len(value) > maxBytes || !json.Valid(value) {
		return nil, ErrInvalid
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(value, &object); err != nil || object == nil {
		return nil, ErrInvalid
	}
	canonical, err := json.Marshal(object)
	if err != nil || len(canonical) > maxBytes {
		return nil, ErrInvalid
	}
	return canonical, nil
}

func knownScanResult(value ScanResult) bool {
	switch value {
	case ScanResultSucceeded, ScanResultFailed, ScanResultCancelled, ScanResultTimedOut, ScanResultUnknown:
		return true
	default:
		return false
	}
}

func knownScanCompleteness(value ScanCompleteness) bool {
	switch value {
	case ScanCompletenessFull, ScanCompletenessPartial, ScanCompletenessIncremental, ScanCompletenessUnknown:
		return true
	default:
		return false
	}
}

func validOptionalText(value string, maxRunes int) bool {
	return value == "" || validText(value, 1, maxRunes)
}

func validText(value string, minRunes, maxRunes int) bool {
	length := utf8.RuneCountInString(value)
	return utf8.ValidString(value) && length >= minRunes && length <= maxRunes &&
		strings.IndexByte(value, 0) < 0
}

func normalizeOptionalTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC().Round(0)
	return &normalized
}
