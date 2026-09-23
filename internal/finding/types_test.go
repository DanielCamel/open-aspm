package finding

import (
	"errors"
	"testing"
	"time"
)

func TestObservationFingerprintIsDeterministicAndExcludesAttemptMetadata(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	base := ObservationSpec{
		ID: "observation-one", WorkspaceID: "workspace-one", ScanID: "scan-one",
		RawArtifactID: "artifact-one", SourceResultIndex: 0, ParserName: "sarif",
		ParserVersion: "1", SourcePointer: "/runs/0/results/0", MessageText: "synthetic",
		Locations: []Location{
			{Ordinal: 1, SourcePointer: "/runs/0/results/0/locations/1"},
			{Ordinal: 0, SourcePointer: "/runs/0/results/0/locations/0"},
		},
		Fingerprints: []SourceFingerprint{
			{Kind: FingerprintPartial, Name: "b", Value: "two"},
			{Kind: FingerprintComplete, Name: "a", Value: "one"},
		},
		RecordedAt: now,
	}
	_, fingerprintA, err := canonicalizeObservation(base)
	if err != nil {
		t.Fatal(err)
	}
	retry := base
	retry.ID = "observation-retry"
	retry.RecordedAt = now.Add(time.Hour)
	retry.Locations = []Location{base.Locations[1], base.Locations[0]}
	retry.Fingerprints = []SourceFingerprint{base.Fingerprints[1], base.Fingerprints[0]}
	_, fingerprintB, err := canonicalizeObservation(retry)
	if err != nil {
		t.Fatal(err)
	}
	if fingerprintA != fingerprintB {
		t.Fatalf("retry fingerprint changed: %x != %x", fingerprintA, fingerprintB)
	}
}

func TestObservationValidationRejectsAmbiguousChildren(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	base := ObservationSpec{
		ID: "observation-one", WorkspaceID: "workspace-one", ScanID: "scan-one",
		RawArtifactID: "artifact-one", SourceResultIndex: 0, ParserName: "sarif",
		ParserVersion: "1", SourcePointer: "/runs/0/results/0", RecordedAt: now,
	}
	duplicateLocations := base
	duplicateLocations.Locations = []Location{
		{Ordinal: 0, SourcePointer: "/runs/0/results/0/locations/0"},
		{Ordinal: 0, SourcePointer: "/runs/0/results/0/locations/0"},
	}
	if _, _, err := canonicalizeObservation(duplicateLocations); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate location error = %v, want ErrInvalid", err)
	}
	duplicateFingerprints := base
	duplicateFingerprints.Fingerprints = []SourceFingerprint{
		{Kind: FingerprintComplete, Name: "same", Value: "one"},
		{Kind: FingerprintComplete, Name: "same", Value: "two"},
	}
	if _, _, err := canonicalizeObservation(duplicateFingerprints); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate fingerprint error = %v, want ErrInvalid", err)
	}
}
