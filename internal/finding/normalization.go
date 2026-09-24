package finding

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNormalizationInvalid  = errors.New("invalid observation normalization input")
	ErrNormalizationConflict = errors.New("normalization conflicts with existing immutable output")
	ErrObservationNotFound   = errors.New("normalization source observation not found")
)

// NormalizedSeverity is a versioned Open ASPM interpretation. It never
// replaces the scanner-provided source severity retained on Observation.
type NormalizedSeverity string

const (
	SeverityUnknown       NormalizedSeverity = "unknown"
	SeverityNotApplicable NormalizedSeverity = "not_applicable"
	SeverityInformational NormalizedSeverity = "informational"
	SeverityLow           NormalizedSeverity = "low"
	SeverityMedium        NormalizedSeverity = "medium"
	SeverityHigh          NormalizedSeverity = "high"
	SeverityCritical      NormalizedSeverity = "critical"
)

// NormalizedCategory remains unknown until supported source semantics justify
// a narrower category. Message text is never used to guess this value.
type NormalizedCategory string

const CategoryUnknown NormalizedCategory = "unknown"

// NormalizedRuleKind states whether RuleID is retained from the source or is
// unavailable. A source rule ID is not claimed to be a cross-scanner taxonomy.
type NormalizedRuleKind string

const (
	RuleUnknown NormalizedRuleKind = "unknown"
	RuleSource  NormalizedRuleKind = "source"
)

// NormalizedLocationKind states which bounded location representation is
// available without inferring a catalog asset identity.
type NormalizedLocationKind string

const (
	LocationUnknown  NormalizedLocationKind = "unknown"
	LocationArtifact NormalizedLocationKind = "artifact"
)

type NormalizedLocation struct {
	Kind        NormalizedLocationKind `json:"kind"`
	URI         string                 `json:"uri,omitempty"`
	URIBaseID   string                 `json:"uri_base_id,omitempty"`
	StartLine   *int                   `json:"start_line,omitempty"`
	StartColumn *int                   `json:"start_column,omitempty"`
	EndLine     *int                   `json:"end_line,omitempty"`
	EndColumn   *int                   `json:"end_column,omitempty"`
}

// NormalizationSpec is one immutable interpretation of one Observation under
// an explicit normalizer version. Effective severity is deliberately absent;
// it belongs to a later attributable risk or policy decision.
type NormalizationSpec struct {
	WorkspaceID       string
	ObservationID     string
	NormalizerName    string
	NormalizerVersion string
	Category          NormalizedCategory
	Severity          NormalizedSeverity
	RuleKind          NormalizedRuleKind
	RuleID            string
	Location          NormalizedLocation
	NormalizedAt      time.Time
}

type StoredNormalization struct {
	WorkspaceID       string
	ObservationID     string
	NormalizerName    string
	NormalizerVersion string
	NormalizedAt      time.Time
	Created           bool
}

type canonicalNormalization struct {
	WorkspaceID       string             `json:"workspace_id"`
	ObservationID     string             `json:"observation_id"`
	NormalizerName    string             `json:"normalizer_name"`
	NormalizerVersion string             `json:"normalizer_version"`
	Category          NormalizedCategory `json:"category"`
	Severity          NormalizedSeverity `json:"severity"`
	RuleKind          NormalizedRuleKind `json:"rule_kind"`
	RuleID            string             `json:"rule_id,omitempty"`
	Location          NormalizedLocation `json:"location"`
}

func canonicalizeNormalization(
	spec NormalizationSpec,
) (canonicalNormalization, [sha256.Size]byte, error) {
	if !validOpaqueID(spec.WorkspaceID) || !validOpaqueID(spec.ObservationID) ||
		!parserNamePattern.MatchString(spec.NormalizerName) ||
		!validText(spec.NormalizerVersion, 1, 64) || spec.NormalizedAt.IsZero() ||
		spec.Category != CategoryUnknown || !knownNormalizedSeverity(spec.Severity) ||
		!validNormalizedRule(spec.RuleKind, spec.RuleID) || !validNormalizedLocation(spec.Location) {
		return canonicalNormalization{}, [sha256.Size]byte{}, ErrNormalizationInvalid
	}
	canonical := canonicalNormalization{
		WorkspaceID: spec.WorkspaceID, ObservationID: spec.ObservationID,
		NormalizerName: spec.NormalizerName, NormalizerVersion: spec.NormalizerVersion,
		Category: spec.Category, Severity: spec.Severity, RuleKind: spec.RuleKind,
		RuleID: spec.RuleID, Location: cloneNormalizedLocation(spec.Location),
	}
	encoded, _ := json.Marshal(canonical)
	return canonical, sha256.Sum256(encoded), nil
}

func knownNormalizedSeverity(value NormalizedSeverity) bool {
	switch value {
	case SeverityUnknown, SeverityNotApplicable, SeverityInformational,
		SeverityLow, SeverityMedium, SeverityHigh, SeverityCritical:
		return true
	default:
		return false
	}
}

func validNormalizedRule(kind NormalizedRuleKind, id string) bool {
	switch kind {
	case RuleUnknown:
		return id == ""
	case RuleSource:
		return validText(id, 1, 512)
	default:
		return false
	}
}

func validNormalizedLocation(location NormalizedLocation) bool {
	switch location.Kind {
	case LocationUnknown:
		return location.URI == "" && location.URIBaseID == "" && location.StartLine == nil &&
			location.StartColumn == nil && location.EndLine == nil && location.EndColumn == nil
	case LocationArtifact:
		return validOptionalBytes(location.URI, 1<<20) && location.URI != "" &&
			validOptionalText(location.URIBaseID, 512) && validOptionalPositive(location.StartLine) &&
			validOptionalPositive(location.StartColumn) && validOptionalPositive(location.EndLine) &&
			validOptionalPositive(location.EndColumn) && validNormalizedRegionOrder(location)
	default:
		return false
	}
}

func validNormalizedRegionOrder(location NormalizedLocation) bool {
	if location.StartLine != nil && location.EndLine != nil {
		if *location.EndLine < *location.StartLine {
			return false
		}
		if *location.EndLine == *location.StartLine && location.StartColumn != nil &&
			location.EndColumn != nil && *location.EndColumn < *location.StartColumn {
			return false
		}
	}
	return true
}

func cloneNormalizedLocation(location NormalizedLocation) NormalizedLocation {
	location.StartLine = cloneOptionalInt(location.StartLine)
	location.StartColumn = cloneOptionalInt(location.StartColumn)
	location.EndLine = cloneOptionalInt(location.EndLine)
	location.EndColumn = cloneOptionalInt(location.EndColumn)
	return location
}
