package sarif

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/spectremi/open-aspm/internal/finding"
)

func TestNormalizeMapsOnlySupportedSARIFSemantics(t *testing.T) {
	line := 42
	laterLine := 99
	normalizedAt := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	observation := finding.ObservationSpec{
		ID: "observation-a", WorkspaceID: "workspace-a", ParserName: "sarif", ParserVersion: "1",
		SourceRuleID: "SQL-001", SourceSeverity: "error", MessageText: "must not drive category",
		Locations: []finding.Location{
			{Ordinal: 2, URI: "src/later.go", StartLine: &laterLine},
			{Ordinal: 0, URI: "src/example.go", URIBaseID: "%SRCROOT%", StartLine: &line},
		},
	}
	result, err := New().Normalize(observation, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if result.Category != finding.CategoryUnknown || result.Severity != finding.SeverityHigh ||
		result.RuleKind != finding.RuleSource || result.RuleID != observation.SourceRuleID ||
		result.Location.Kind != finding.LocationArtifact || result.Location.URI != "src/example.go" ||
		result.Location.URIBaseID != "%SRCROOT%" || result.Location.StartLine == nil ||
		*result.Location.StartLine != line || !result.NormalizedAt.Equal(normalizedAt) {
		t.Fatalf("Normalize() = %+v", result)
	}
	line = 7
	if *result.Location.StartLine != 42 {
		t.Fatal("Normalize() retained a mutable source pointer")
	}
	line = 42
	reordered := observation
	reordered.MessageText = "different untrusted presentation text"
	reordered.Locations = []finding.Location{observation.Locations[1], observation.Locations[0]}
	repeated, err := New().Normalize(reordered, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, repeated) {
		t.Fatalf("deterministic normalization changed: %+v != %+v", result, repeated)
	}
}

func TestNormalizeSeverityPreservesUnknown(t *testing.T) {
	tests := []struct {
		source string
		want   finding.NormalizedSeverity
	}{
		{source: "", want: finding.SeverityUnknown},
		{source: "vendor-critical", want: finding.SeverityUnknown},
		{source: "none", want: finding.SeverityNotApplicable},
		{source: "note", want: finding.SeverityLow},
		{source: "warning", want: finding.SeverityMedium},
		{source: "error", want: finding.SeverityHigh},
	}
	for _, test := range tests {
		if got := normalizeSeverity(test.source); got != test.want {
			t.Errorf("normalizeSeverity(%q) = %q, want %q", test.source, got, test.want)
		}
	}
}

func TestNormalizeKeepsUnavailableRuleAndLocationUnknown(t *testing.T) {
	result, err := New().Normalize(finding.ObservationSpec{
		ID: "observation-a", WorkspaceID: "workspace-a", ParserName: "sarif", ParserVersion: "1",
		Locations: []finding.Location{{Ordinal: 0}},
	}, time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if result.RuleKind != finding.RuleUnknown || result.RuleID != "" ||
		result.Location.Kind != finding.LocationUnknown || result.Location.URI != "" ||
		result.Severity != finding.SeverityUnknown {
		t.Fatalf("Normalize() invented unavailable semantics: %+v", result)
	}
}

func TestNormalizeRejectsNonSARIFObservation(t *testing.T) {
	for _, observation := range []finding.ObservationSpec{
		{ParserName: "other", ParserVersion: "1"},
		{ParserName: "sarif", ParserVersion: "future"},
	} {
		_, err := New().Normalize(observation, time.Now())
		if !errors.Is(err, ErrUnsupportedObservation) {
			t.Fatalf("Normalize(%+v) error = %v, want ErrUnsupportedObservation", observation, err)
		}
	}
}
