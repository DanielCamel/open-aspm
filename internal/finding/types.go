// Package finding owns immutable scanner observations and, in later slices,
// stable finding identity and technical lifecycle.
package finding

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalid             = errors.New("invalid observation input")
	ErrObservationConflict = errors.New("observation source record conflicts with existing immutable observation")
	ErrScanNotFound        = errors.New("observation scan not found")
)

var parserNamePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)

// FingerprintKind distinguishes SARIF fingerprints from partialFingerprints.
// These are source facts, not Open ASPM finding-correlation fingerprints.
type FingerprintKind string

const (
	FingerprintComplete FingerprintKind = "complete"
	FingerprintPartial  FingerprintKind = "partial"
)

type SourceFingerprint struct {
	Kind  FingerprintKind `json:"kind"`
	Name  string          `json:"name"`
	Value string          `json:"value"`
}

type Location struct {
	Ordinal       int    `json:"ordinal"`
	SourcePointer string `json:"source_pointer"`
	URI           string `json:"uri,omitempty"`
	URIBaseID     string `json:"uri_base_id,omitempty"`
	ArtifactIndex *int   `json:"artifact_index,omitempty"`
	StartLine     *int   `json:"start_line,omitempty"`
	StartColumn   *int   `json:"start_column,omitempty"`
	EndLine       *int   `json:"end_line,omitempty"`
	EndColumn     *int   `json:"end_column,omitempty"`
}

// ObservationSpec is one immutable scanner statement. SourceSeverity remains
// separate from later normalized and effective severity.
type ObservationSpec struct {
	ID                    string
	WorkspaceID           string
	ScanID                string
	RawArtifactID         string
	SourceResultIndex     int
	ParserName            string
	ParserVersion         string
	SourcePointer         string
	SourceGUID            string
	SourceCorrelationGUID string
	SourceRuleID          string
	SourceRuleIndex       *int
	SourceSeverity        string
	SourceKind            string
	SourceBaselineState   string
	MessageID             string
	MessageText           string
	MessageMarkdown       string
	MessageArguments      []string
	Locations             []Location
	Fingerprints          []SourceFingerprint
	ObservedAt            *time.Time
	RecordedAt            time.Time
}

// StoredObservation reports the authoritative ID. An exact replay returns the
// originally stored ID rather than the retry's candidate ID.
type StoredObservation struct {
	ID            string
	WorkspaceID   string
	ScanID        string
	RawArtifactID string
	ReceivedAt    time.Time
	RecordedAt    time.Time
	Created       bool
}

type canonicalObservation struct {
	WorkspaceID           string              `json:"workspace_id"`
	ScanID                string              `json:"scan_id"`
	RawArtifactID         string              `json:"raw_artifact_id"`
	SourceResultIndex     int                 `json:"source_result_index"`
	ParserName            string              `json:"parser_name"`
	ParserVersion         string              `json:"parser_version"`
	SourcePointer         string              `json:"source_pointer"`
	SourceGUID            string              `json:"source_guid,omitempty"`
	SourceCorrelationGUID string              `json:"source_correlation_guid,omitempty"`
	SourceRuleID          string              `json:"source_rule_id,omitempty"`
	SourceRuleIndex       *int                `json:"source_rule_index,omitempty"`
	SourceSeverity        string              `json:"source_severity,omitempty"`
	SourceKind            string              `json:"source_kind,omitempty"`
	SourceBaselineState   string              `json:"source_baseline_state,omitempty"`
	MessageID             string              `json:"message_id,omitempty"`
	MessageText           string              `json:"message_text,omitempty"`
	MessageMarkdown       string              `json:"message_markdown,omitempty"`
	MessageArguments      []string            `json:"message_arguments"`
	Locations             []Location          `json:"locations"`
	Fingerprints          []SourceFingerprint `json:"fingerprints"`
	ObservedAt            *time.Time          `json:"observed_at,omitempty"`
}

func canonicalizeObservation(spec ObservationSpec) (canonicalObservation, [sha256.Size]byte, error) {
	if !validOpaqueID(spec.ID) || !validOpaqueID(spec.WorkspaceID) || !validOpaqueID(spec.ScanID) ||
		!validOpaqueID(spec.RawArtifactID) || spec.SourceResultIndex < 0 || spec.RecordedAt.IsZero() ||
		!parserNamePattern.MatchString(spec.ParserName) || !validText(spec.ParserVersion, 1, 64) ||
		!validText(spec.SourcePointer, 1, 1024) {
		return canonicalObservation{}, [sha256.Size]byte{}, ErrInvalid
	}
	if !validOptionalText(spec.SourceGUID, 128) || !validOptionalText(spec.SourceCorrelationGUID, 128) ||
		!validOptionalText(spec.SourceRuleID, 512) || !validOptionalIndex(spec.SourceRuleIndex) ||
		!validOptionalText(spec.SourceSeverity, 64) || !validOptionalText(spec.SourceKind, 64) ||
		!validOptionalText(spec.SourceBaselineState, 64) || !validOptionalText(spec.MessageID, 512) ||
		!validOptionalBytes(spec.MessageText, 1<<20) || !validOptionalBytes(spec.MessageMarkdown, 1<<20) ||
		len(spec.MessageArguments) > 1000 || len(spec.Locations) > 1000 || len(spec.Fingerprints) > 2000 {
		return canonicalObservation{}, [sha256.Size]byte{}, ErrInvalid
	}
	arguments := make([]string, len(spec.MessageArguments))
	copy(arguments, spec.MessageArguments)
	for _, argument := range arguments {
		if !utf8.ValidString(argument) || len(argument) > 1<<20 || strings.IndexByte(argument, 0) >= 0 {
			return canonicalObservation{}, [sha256.Size]byte{}, ErrInvalid
		}
	}
	locations := append([]Location(nil), spec.Locations...)
	sort.Slice(locations, func(i, j int) bool { return locations[i].Ordinal < locations[j].Ordinal })
	for index, location := range locations {
		if !validLocation(location) || (index > 0 && locations[index-1].Ordinal == location.Ordinal) {
			return canonicalObservation{}, [sha256.Size]byte{}, ErrInvalid
		}
	}
	fingerprints := append([]SourceFingerprint(nil), spec.Fingerprints...)
	sort.Slice(fingerprints, func(i, j int) bool {
		if fingerprints[i].Kind == fingerprints[j].Kind {
			return fingerprints[i].Name < fingerprints[j].Name
		}
		return fingerprints[i].Kind < fingerprints[j].Kind
	})
	for index, fingerprint := range fingerprints {
		if !validFingerprint(fingerprint) || (index > 0 && fingerprints[index-1].Kind == fingerprint.Kind &&
			fingerprints[index-1].Name == fingerprint.Name) {
			return canonicalObservation{}, [sha256.Size]byte{}, ErrInvalid
		}
	}
	observation := canonicalObservation{
		WorkspaceID: spec.WorkspaceID, ScanID: spec.ScanID, RawArtifactID: spec.RawArtifactID,
		SourceResultIndex: spec.SourceResultIndex, ParserName: spec.ParserName,
		ParserVersion: spec.ParserVersion, SourcePointer: spec.SourcePointer, SourceGUID: spec.SourceGUID,
		SourceCorrelationGUID: spec.SourceCorrelationGUID, SourceRuleID: spec.SourceRuleID,
		SourceRuleIndex: cloneOptionalInt(spec.SourceRuleIndex), SourceSeverity: spec.SourceSeverity,
		SourceKind: spec.SourceKind, SourceBaselineState: spec.SourceBaselineState, MessageID: spec.MessageID,
		MessageText: spec.MessageText, MessageMarkdown: spec.MessageMarkdown, MessageArguments: arguments,
		Locations: locations, Fingerprints: fingerprints, ObservedAt: normalizeOptionalTime(spec.ObservedAt),
	}
	encoded, _ := json.Marshal(observation)
	return observation, sha256.Sum256(encoded), nil
}

func validLocation(location Location) bool {
	return location.Ordinal >= 0 && validText(location.SourcePointer, 1, 1024) &&
		validOptionalBytes(location.URI, 1<<20) && validOptionalText(location.URIBaseID, 512) &&
		validOptionalIndex(location.ArtifactIndex) && validOptionalPositive(location.StartLine) &&
		validOptionalPositive(location.StartColumn) && validOptionalPositive(location.EndLine) &&
		validOptionalPositive(location.EndColumn)
}

func validFingerprint(fingerprint SourceFingerprint) bool {
	return (fingerprint.Kind == FingerprintComplete || fingerprint.Kind == FingerprintPartial) &&
		validText(fingerprint.Name, 1, 512) && fingerprint.Value != "" && validOptionalBytes(fingerprint.Value, 1<<20)
}

func validOpaqueID(value string) bool {
	length := utf8.RuneCountInString(value)
	return utf8.ValidString(value) && length >= 3 && length <= 128 &&
		strings.TrimSpace(value) == value && strings.IndexByte(value, 0) < 0
}

func validText(value string, minRunes, maxRunes int) bool {
	length := utf8.RuneCountInString(value)
	return utf8.ValidString(value) && length >= minRunes && length <= maxRunes && strings.IndexByte(value, 0) < 0
}

func validOptionalText(value string, maxRunes int) bool {
	return value == "" || validText(value, 1, maxRunes)
}

func validOptionalBytes(value string, maxBytes int) bool {
	return value == "" || (utf8.ValidString(value) && len(value) <= maxBytes && strings.IndexByte(value, 0) < 0)
}

func validOptionalIndex(value *int) bool { return value == nil || *value >= 0 }

func validOptionalPositive(value *int) bool { return value == nil || *value > 0 }

func cloneOptionalInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func normalizeOptionalTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC().Round(0)
	return &normalized
}
