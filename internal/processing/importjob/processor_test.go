package importjob

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/spectremi/open-aspm/internal/ingestion"
	"github.com/spectremi/open-aspm/internal/jobqueue"
	"github.com/spectremi/open-aspm/internal/parsing/sarif"
)

func TestDecodeJobRequiresExactInternalContract(t *testing.T) {
	job := jobqueue.Job{
		WorkspaceID: "workspace-a", OperationID: "operation-a", InitiatingPrincipalID: "principal-a",
		SystemCapability: ingestion.CapabilityImportsProcess,
		Kind:             ingestion.ImportProcessJobKind, SchemaVersion: ingestion.ImportProcessJobSchema,
		Payload: json.RawMessage(`{"import_id":"import-a"}`),
	}
	identity, err := decodeJob(job)
	if err != nil {
		t.Fatal(err)
	}
	if identity.ImportID != "import-a" || identity.OperationID != job.OperationID {
		t.Fatalf("decodeJob() = %+v", identity)
	}
	job.Payload = json.RawMessage(`{"import_id":"import-a","storage_key":"must-not-be-accepted"}`)
	if _, err := decodeJob(job); err == nil {
		t.Fatal("decodeJob() accepted an undeclared payload field")
	}
}

func TestMapScanPreservesUnknownCoverageAndScannerOutcome(t *testing.T) {
	source := ingestion.ProcessingSource{
		ProcessingTarget: ingestion.ProcessingTarget{
			WorkspaceID: "workspace-a", ImportID: "import-a", ApplicationID: "application-a",
		},
		RawArtifactID: "artifact-a",
	}
	document := sarif.Document{Parser: sarif.ParserIdentity{Name: sarif.Format, Version: sarif.ParserVersion}}
	recordedAt := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		invocations []sarif.Invocation
		want        ingestion.ScanResult
	}{
		{name: "missing is unknown", want: ingestion.ScanResultUnknown},
		{name: "all successful", invocations: []sarif.Invocation{{ExecutionSuccessful: true}}, want: ingestion.ScanResultSucceeded},
		{name: "any failure", invocations: []sarif.Invocation{{ExecutionSuccessful: true}, {ExecutionSuccessful: false}}, want: ingestion.ScanResultFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := sarif.Run{
				Index: 0, SourcePointer: "/runs/0", Tool: sarif.Tool{Name: "Synthetic Scanner"},
				Invocations: test.invocations,
			}
			scan := mapScan(source, document, run, "scan-a", recordedAt)
			if scan.Result != test.want || scan.Completeness != ingestion.ScanCompletenessUnknown ||
				scan.Scope.ScannerFamily != "unknown" || scan.Scope.AnalysisKind != "unknown" {
				t.Fatalf("mapScan() = result %q completeness %q scope %+v", scan.Result, scan.Completeness, scan.Scope)
			}
		})
	}
}

func TestParseSourceTimeDoesNotInventInvalidTime(t *testing.T) {
	if got := parseSourceTime("not-a-time"); got != nil {
		t.Fatalf("parseSourceTime() = %v, want unknown", got)
	}
	want := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	if got := parseSourceTime("2026-09-23T10:00:00Z"); got == nil || !got.Equal(want) {
		t.Fatalf("parseSourceTime() = %v, want %v", got, want)
	}
}
