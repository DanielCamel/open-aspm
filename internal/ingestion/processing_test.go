package ingestion

import (
	"errors"
	"testing"
	"time"
)

func TestParseOutputFingerprintIsDeterministicAndExcludesRecordedAt(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	base := ParseOutputSpec{
		WorkspaceID: "workspace-a", ImportID: "import-a", RawArtifactID: "artifact-a",
		FormatName: "sarif", FormatVersion: "2.1.0", ParserName: "sarif", ParserVersion: "1",
		Warnings: []ParseWarning{{
			Code: "unsupported_fields", SourcePointer: "", FieldCount: 2,
			Fields: []string{"artifacts", "originalUriBaseIds"},
		}},
		RecordedAt: now,
	}
	_, fingerprintA, err := canonicalizeParseOutput(base)
	if err != nil {
		t.Fatal(err)
	}
	replay := base
	replay.RecordedAt = now.Add(time.Hour)
	_, fingerprintB, err := canonicalizeParseOutput(replay)
	if err != nil {
		t.Fatal(err)
	}
	if fingerprintA != fingerprintB {
		t.Fatalf("replay fingerprint changed: %x != %x", fingerprintA, fingerprintB)
	}
}

func TestProcessingFailureUsesPublicProblemCodeSyntax(t *testing.T) {
	if validFailure(ProcessingFailure{Code: "contains_underscore", Title: "Invalid"}) {
		t.Fatal("validFailure() accepted a code rejected by operations schema")
	}
	if !validFailure(ProcessingFailure{Code: "sarif-invalid-sarif", Title: "SARIF invalid"}) {
		t.Fatal("validFailure() rejected a stable public code")
	}
}

func TestProcessingIdentityRequiresInternalCapability(t *testing.T) {
	identity := ProcessingIdentity{
		WorkspaceID: "workspace-a", OperationID: "operation-a", ImportID: "import-a",
		PrincipalID: "principal-a", JobCapability: CapabilityImportsProcess,
	}
	if err := validateProcessingIdentity(identity); err != nil {
		t.Fatal(err)
	}
	identity.JobCapability = CapabilityImportsUpload
	if err := validateProcessingIdentity(identity); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong capability error = %v, want ErrInvalid", err)
	}
}
