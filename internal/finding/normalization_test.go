package finding

import (
	"errors"
	"testing"
	"time"
)

func TestNormalizationFingerprintIsDeterministicAndExcludesRecordedTime(t *testing.T) {
	line := 12
	base := NormalizationSpec{
		WorkspaceID: "workspace-a", ObservationID: "observation-a",
		NormalizerName: "sarif", NormalizerVersion: "1",
		Category: CategoryUnknown, Severity: SeverityMedium,
		RuleKind: RuleSource, RuleID: "OA1001",
		Location:     NormalizedLocation{Kind: LocationArtifact, URI: "src/handler.go", StartLine: &line},
		NormalizedAt: time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC),
	}
	canonicalA, fingerprintA, err := canonicalizeNormalization(base)
	if err != nil {
		t.Fatal(err)
	}
	retry := base
	retry.NormalizedAt = base.NormalizedAt.Add(time.Hour)
	canonicalB, fingerprintB, err := canonicalizeNormalization(retry)
	if err != nil {
		t.Fatal(err)
	}
	if fingerprintA != fingerprintB || canonicalA.Location.StartLine == base.Location.StartLine ||
		canonicalB.Location.StartLine == retry.Location.StartLine {
		t.Fatalf("normalization replay was not canonical: %x != %x", fingerprintA, fingerprintB)
	}
}

func TestNormalizationValidationPreservesExplicitUnknown(t *testing.T) {
	base := NormalizationSpec{
		WorkspaceID: "workspace-a", ObservationID: "observation-a",
		NormalizerName: "sarif", NormalizerVersion: "1",
		Category: CategoryUnknown, Severity: SeverityUnknown, RuleKind: RuleUnknown,
		Location:     NormalizedLocation{Kind: LocationUnknown},
		NormalizedAt: time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC),
	}
	if _, _, err := canonicalizeNormalization(base); err != nil {
		t.Fatalf("explicit unknown normalization rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*NormalizationSpec)
	}{
		{name: "unknown rule with identifier", mutate: func(spec *NormalizationSpec) { spec.RuleID = "invented" }},
		{name: "source rule without identifier", mutate: func(spec *NormalizationSpec) { spec.RuleKind = RuleSource }},
		{name: "unknown location with URI", mutate: func(spec *NormalizationSpec) { spec.Location.URI = "src/example.go" }},
		{name: "artifact location without URI", mutate: func(spec *NormalizationSpec) { spec.Location.Kind = LocationArtifact }},
		{name: "backwards region", mutate: func(spec *NormalizationSpec) {
			start, end := 10, 9
			spec.Location = NormalizedLocation{Kind: LocationArtifact, URI: "src/example.go", StartLine: &start, EndLine: &end}
		}},
		{name: "unsupported category", mutate: func(spec *NormalizationSpec) { spec.Category = "code" }},
		{name: "unsupported severity", mutate: func(spec *NormalizationSpec) { spec.Severity = "urgent" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := base
			test.mutate(&spec)
			if _, _, err := canonicalizeNormalization(spec); !errors.Is(err, ErrNormalizationInvalid) {
				t.Fatalf("canonicalizeNormalization() error = %v, want ErrNormalizationInvalid", err)
			}
		})
	}
}
