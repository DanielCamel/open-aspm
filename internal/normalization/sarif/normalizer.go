// Package sarif normalizes supported SARIF Observation fields into bounded,
// versioned Open ASPM semantics without inferring identity or category.
package sarif

import (
	"errors"
	"time"

	"github.com/spectremi/open-aspm/internal/finding"
	parsingsarif "github.com/spectremi/open-aspm/internal/parsing/sarif"
)

const (
	Name    = "sarif"
	Version = "1"
)

var ErrUnsupportedObservation = errors.New("unsupported observation for SARIF normalization")

type Normalizer struct{}

func New() *Normalizer { return &Normalizer{} }

// Normalize maps only semantics justified by the retained SARIF source fields.
// It does not inspect message text, resolve assets, or calculate finding identity.
func (normalizer *Normalizer) Normalize(
	observation finding.ObservationSpec,
	normalizedAt time.Time,
) (finding.NormalizationSpec, error) {
	if normalizer == nil || observation.ParserName != parsingsarif.Format ||
		observation.ParserVersion != parsingsarif.ParserVersion {
		return finding.NormalizationSpec{}, ErrUnsupportedObservation
	}
	result := finding.NormalizationSpec{
		WorkspaceID: observation.WorkspaceID, ObservationID: observation.ID,
		NormalizerName: Name, NormalizerVersion: Version,
		Category: finding.CategoryUnknown, Severity: normalizeSeverity(observation.SourceSeverity),
		RuleKind: finding.RuleUnknown, Location: finding.NormalizedLocation{Kind: finding.LocationUnknown},
		NormalizedAt: normalizedAt,
	}
	if observation.SourceRuleID != "" {
		result.RuleKind = finding.RuleSource
		result.RuleID = observation.SourceRuleID
	}
	if location, ok := primaryArtifactLocation(observation.Locations); ok {
		result.Location = location
	}
	return result, nil
}

func normalizeSeverity(source string) finding.NormalizedSeverity {
	switch source {
	case "none":
		return finding.SeverityNotApplicable
	case "note":
		return finding.SeverityLow
	case "warning":
		return finding.SeverityMedium
	case "error":
		return finding.SeverityHigh
	default:
		return finding.SeverityUnknown
	}
}

func primaryArtifactLocation(locations []finding.Location) (finding.NormalizedLocation, bool) {
	if len(locations) == 0 {
		return finding.NormalizedLocation{}, false
	}
	primary := locations[0]
	for _, candidate := range locations[1:] {
		if candidate.Ordinal < primary.Ordinal {
			primary = candidate
		}
	}
	if primary.URI == "" {
		return finding.NormalizedLocation{}, false
	}
	return finding.NormalizedLocation{
		Kind: finding.LocationArtifact, URI: primary.URI, URIBaseID: primary.URIBaseID,
		StartLine: cloneInt(primary.StartLine), StartColumn: cloneInt(primary.StartColumn),
		EndLine: cloneInt(primary.EndLine), EndColumn: cloneInt(primary.EndColumn),
	}, true
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
