package ingestion

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestScanFingerprintIsDeterministicAndExcludesAttemptMetadata(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	base := ScanSpec{
		ID: "scan-one", WorkspaceID: "workspace-one", ImportID: "import-one",
		ApplicationID: "application-one", RawArtifactID: "artifact-one", SourceRunIndex: 0,
		Result: ScanResultUnknown, Completeness: ScanCompletenessUnknown,
		ScannerName: "Example Scanner", ParserName: "sarif", ParserVersion: "1",
		SourcePointer: "/runs/0", RecordedAt: now,
		Scope: ScanScope{
			SchemaVersion: 1, ScannerFamily: "unknown", AnalysisKind: "unknown",
			CoverageMetadata: json.RawMessage(`{"b":2,"a":1}`),
		},
	}
	_, _, scanA, scopeA, err := canonicalizeScanSpec(base)
	if err != nil {
		t.Fatal(err)
	}
	retry := base
	retry.ID = "scan-retry"
	retry.RecordedAt = now.Add(time.Hour)
	retry.Scope.CoverageMetadata = json.RawMessage(`{"a":1,"b":2}`)
	_, _, scanB, scopeB, err := canonicalizeScanSpec(retry)
	if err != nil {
		t.Fatal(err)
	}
	if scanA != scanB || scopeA != scopeB {
		t.Fatalf("retry fingerprints changed: scan %x/%x scope %x/%x", scanA, scanB, scopeA, scopeB)
	}
}

func TestScanValidationPreservesExplicitUnknown(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	spec := ScanSpec{
		ID: "scan-one", WorkspaceID: "workspace-one", ImportID: "import-one",
		ApplicationID: "application-one", RawArtifactID: "artifact-one", SourceRunIndex: 0,
		Result: ScanResultUnknown, Completeness: ScanCompletenessUnknown,
		ScannerName: "Example Scanner", ParserName: "sarif", ParserVersion: "1",
		SourcePointer: "/runs/0", RecordedAt: now,
		Scope: ScanScope{SchemaVersion: 1, ScannerFamily: "unknown", AnalysisKind: "unknown"},
	}
	if _, _, _, _, err := canonicalizeScanSpec(spec); err != nil {
		t.Fatalf("explicit unknown scan rejected: %v", err)
	}
	spec.Completeness = ""
	if _, _, _, _, err := canonicalizeScanSpec(spec); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing completeness error = %v, want ErrInvalid", err)
	}
}
