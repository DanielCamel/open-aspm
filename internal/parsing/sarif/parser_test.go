package sarif

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var fixtureRoot = filepath.Join("..", "..", "..", "testdata", "sarif")

func TestRepresentativeFixtureProducesDeterministicSourceRecords(t *testing.T) {
	parser := newTestParser(t)
	source := readParserFixture(t, "valid", "representative.sarif.json")

	first, err := parser.Parse(context.Background(), bytes.NewReader(source))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	second, err := parser.Parse(context.Background(), bytes.NewReader(source))
	if err != nil {
		t.Fatalf("second Parse() error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same bytes produced different documents:\nfirst=%+v\nsecond=%+v", first, second)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil || !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("deterministic JSON differs: %s %s, %v", firstJSON, secondJSON, err)
	}

	if first.Format != Format || first.FormatVersion != FormatVersion ||
		first.Parser != (ParserIdentity{Name: Format, Version: ParserVersion}) || len(first.Runs) != 2 {
		t.Fatalf("document metadata = %+v", first)
	}
	firstRun := first.Runs[0]
	if firstRun.SourcePointer != "/runs/0" || firstRun.Tool.Name != "Open ASPM Synthetic SAST" ||
		firstRun.Tool.Version != "1.0.0" || len(firstRun.Rules) != 2 || len(firstRun.Results) != 2 {
		t.Fatalf("first run = %+v", firstRun)
	}
	result := firstRun.Results[0]
	if result.SourcePointer != "/runs/0/results/0" || result.RuleID != "OA1001" ||
		result.RuleIndex == nil || *result.RuleIndex != 0 || result.Level != "warning" ||
		result.Message.Text != "Synthetic data flow used only to exercise ingestion." ||
		len(result.Locations) != 2 || len(result.Fingerprints) != 1 ||
		result.Fingerprints[0] != (NamedValue{Name: "open-aspm/v1", Value: "synthetic-fingerprint-001"}) ||
		len(result.PartialFingerprints) != 1 {
		t.Fatalf("first result = %+v", result)
	}
	location := result.Locations[0]
	if location.URI != "src/handler.go" || location.URIBaseID != "%SRCROOT%" ||
		location.ArtifactIndex == nil || *location.ArtifactIndex != 0 ||
		location.Region == nil || location.Region.StartLine == nil || *location.Region.StartLine != 12 {
		t.Fatalf("first location = %+v", location)
	}
	if len(first.Warnings) != 1 || first.Warnings[0].Path != "/runs/0" ||
		!reflect.DeepEqual(first.Warnings[0].Fields, []string{"artifacts", "originalUriBaseIds"}) ||
		first.WarningsTruncated {
		t.Fatalf("warnings = %+v, truncated = %t", first.Warnings, first.WarningsTruncated)
	}
}

func TestMinimalFixturePreservesUnknownInsteadOfInventingCoverage(t *testing.T) {
	parser := newTestParser(t)
	document, err := parser.Parse(
		context.Background(),
		bytes.NewReader(readParserFixture(t, "valid", "minimal.sarif.json")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(document.Runs) != 1 || document.Runs[0].Tool.Name != "Open ASPM Synthetic Scanner" ||
		len(document.Runs[0].Invocations) != 0 || len(document.Runs[0].Results) != 0 ||
		len(document.Warnings) != 0 {
		t.Fatalf("minimal document = %+v", document)
	}
}

func TestScannerFailureIsEvidenceNotParserFailure(t *testing.T) {
	parser := newTestParser(t)
	source := `{
		"version":"2.1.0",
		"runs":[{
			"tool":{"driver":{"name":"Synthetic Scanner"}},
			"invocations":[{
				"executionSuccessful":false,
				"exitCode":2,
				"exitCodeDescription":"Synthetic scanner reported failure"
			}]
		}]
	}`
	document, err := parser.Parse(context.Background(), strings.NewReader(source))
	if err != nil {
		t.Fatalf("Parse() treated scanner failure as parser error: %v", err)
	}
	invocation := document.Runs[0].Invocations[0]
	if invocation.ExecutionSuccessful || invocation.ExitCode == nil || *invocation.ExitCode != 2 {
		t.Fatalf("invocation = %+v", invocation)
	}
}

func TestHostileMarkupRemainsUninterpretedData(t *testing.T) {
	parser := newTestParser(t)
	message := `<script>window.location='https://example.com/not-executed'</script>`
	source := `{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"Synthetic Scanner"}},` +
		`"results":[{"message":{"text":"Synthetic fallback","markdown":` + strconvQuote(message) + `}}]}]}`
	document, err := parser.Parse(context.Background(), strings.NewReader(source))
	if err != nil {
		t.Fatal(err)
	}
	if document.Runs[0].Results[0].Message.Markdown != message {
		t.Fatalf("markdown was changed or interpreted: %q", document.Runs[0].Results[0].Message.Markdown)
	}
}

func TestFingerprintsAreSortedAndAbsentOptionalObjectsStayAbsent(t *testing.T) {
	parser := newTestParser(t)
	source := `{
		"version":"2.1.0",
		"runs":[{
			"tool":{"driver":{"name":"Synthetic Scanner"}},
			"results":[{
				"message":{"text":"Synthetic result"},
				"fingerprints":{"z/v1":"last","a/v1":"first"},
				"partialFingerprints":{"middle/v1":"middle"}
			}]
		}]
	}`
	document, err := parser.Parse(context.Background(), strings.NewReader(source))
	if err != nil {
		t.Fatal(err)
	}
	run := document.Runs[0]
	if run.AutomationDetails != nil {
		t.Fatalf("missing automation details became %+v", run.AutomationDetails)
	}
	result := run.Results[0]
	want := []NamedValue{{Name: "a/v1", Value: "first"}, {Name: "z/v1", Value: "last"}}
	if !reflect.DeepEqual(result.Fingerprints, want) {
		t.Fatalf("fingerprints = %+v, want %+v", result.Fingerprints, want)
	}
}

func TestNullRunsIsValidButDoesNotClaimACompletedScan(t *testing.T) {
	parser := newTestParser(t)
	document, err := parser.Parse(
		context.Background(),
		strings.NewReader(`{"version":"2.1.0","runs":null}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(document.Runs) != 0 {
		t.Fatalf("runs = %+v, want empty", document.Runs)
	}
}

func TestInvalidFixturesHaveStableSafeFailures(t *testing.T) {
	tests := []struct {
		name      string
		file      string
		configure func(*Limits)
		want      ErrorCode
	}{
		{name: "malformed", file: "malformed-json.sarif.json", want: ErrorMalformedJSON},
		{name: "unsupported version", file: "unsupported-version.sarif.json", want: ErrorUnsupportedVersion},
		{name: "deeply nested", file: "deeply-nested.sarif.json", configure: func(limits *Limits) {
			limits.MaxJSONDepth = 16
		}, want: ErrorLimitExceeded},
		{name: "oversized", file: "oversized.sarif.json", configure: func(limits *Limits) {
			limits.MaxBytes = 1 << 10
		}, want: ErrorInputTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := DefaultLimits()
			if test.configure != nil {
				test.configure(&limits)
			}
			parser, err := New(limits)
			if err != nil {
				t.Fatal(err)
			}
			_, err = parser.Parse(
				context.Background(),
				bytes.NewReader(readParserFixture(t, "invalid", test.file)),
			)
			assertErrorCode(t, err, test.want)
		})
	}
}

func TestParserRejectsAmbiguousAndInvalidJSON(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  ErrorCode
	}{
		{name: "duplicate key", input: []byte(`{"version":"2.1.0","version":"2.1.0","runs":[]}`), want: ErrorMalformedJSON},
		{name: "trailing value", input: []byte(`{"version":"2.1.0","runs":[]} {}`), want: ErrorMalformedJSON},
		{name: "invalid utf8", input: append([]byte(`{"version":"2.1.0","runs":[],"x":"`), 0xff, '"', '}'), want: ErrorInvalidEncoding},
		{name: "array root", input: []byte(`[]`), want: ErrorInvalidSARIF},
		{name: "missing runs", input: []byte(`{"version":"2.1.0"}`), want: ErrorInvalidSARIF},
		{name: "missing driver name", input: []byte(`{"version":"2.1.0","runs":[{"tool":{"driver":{}}}]}`), want: ErrorInvalidSARIF},
		{name: "invalid result level", input: []byte(`{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"x"}},"results":[{"level":"critical","message":{"text":"x"}}]}]}`), want: ErrorInvalidSARIF},
		{name: "invalid region", input: []byte(`{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"x"}},"results":[{"message":{"text":"x"},"locations":[{"physicalLocation":{"region":{"startLine":0}}}]}]}]}`), want: ErrorInvalidSARIF},
	}
	parser := newTestParser(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parser.Parse(context.Background(), bytes.NewReader(test.input))
			assertErrorCode(t, err, test.want)
		})
	}
}

func TestParserEnforcesDomainCountsAndWarningBounds(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxRuns = 1
	parser, err := New(limits)
	if err != nil {
		t.Fatal(err)
	}
	_, err = parser.Parse(context.Background(), strings.NewReader(
		`{"version":"2.1.0","runs":[`+
			`{"tool":{"driver":{"name":"a"}}},{"tool":{"driver":{"name":"b"}}}]}`,
	))
	assertErrorCode(t, err, ErrorLimitExceeded)

	limits = DefaultLimits()
	limits.MaxWarnings = 1
	limits.MaxUnsupportedFieldsPerWarning = 1
	parser, err = New(limits)
	if err != nil {
		t.Fatal(err)
	}
	document, err := parser.Parse(context.Background(), strings.NewReader(
		`{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"a","z":1}},"y":2}],"x":1,"w":2}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(document.Warnings) != 1 || !document.WarningsTruncated ||
		document.Warnings[0].FieldCount != 2 || !document.Warnings[0].FieldsTruncated ||
		!reflect.DeepEqual(document.Warnings[0].Fields, []string{"w"}) {
		t.Fatalf("warnings = %+v, truncated = %t", document.Warnings, document.WarningsTruncated)
	}
}

func TestParserEnforcesGenericStructuralLimits(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		configure func(*Limits)
	}{
		{
			name:  "object members",
			input: `{"version":"2.1.0","runs":[],"extra":true}`,
			configure: func(limits *Limits) {
				limits.MaxObjectMembers = 2
				limits.MaxFingerprintsPerResult = 2
				limits.MaxUnsupportedFieldsPerWarning = 2
			},
		},
		{
			name:  "array elements",
			input: `{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"a"}}},{"tool":{"driver":{"name":"b"}}}]}`,
			configure: func(limits *Limits) {
				limits.MaxArrayElements = 1
				limits.MaxRuns = 1
				limits.MaxRulesPerRun = 1
				limits.MaxResultsPerRun = 1
				limits.MaxInvocationsPerRun = 1
				limits.MaxLocationsPerResult = 1
			},
		},
		{
			name:  "string bytes",
			input: `{"version":"2.1.0","runs":[]}`,
			configure: func(limits *Limits) {
				limits.MaxStringBytes = 4
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := DefaultLimits()
			test.configure(&limits)
			parser, err := New(limits)
			if err != nil {
				t.Fatal(err)
			}
			_, err = parser.Parse(context.Background(), strings.NewReader(test.input))
			assertErrorCode(t, err, ErrorLimitExceeded)
		})
	}
}

func TestParserCancellationAndReadFailureAreClassified(t *testing.T) {
	parser := newTestParser(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := parser.Parse(cancelled, strings.NewReader(`{"version":"2.1.0","runs":[]}`))
	assertErrorCode(t, err, ErrorCancelled)

	_, err = parser.Parse(context.Background(), failingReader{})
	assertErrorCode(t, err, ErrorInputRead)
}

func TestErrorsNeverEchoScannerControlledValues(t *testing.T) {
	parser := newTestParser(t)
	secretShaped := "EXAMPLE_NOT_A_SECRET"
	_, err := parser.Parse(context.Background(), strings.NewReader(
		`{"version":`+strconvQuote(secretShaped)+`,"runs":[]}`,
	))
	assertErrorCode(t, err, ErrorUnsupportedVersion)
	if strings.Contains(err.Error(), secretShaped) {
		t.Fatalf("error disclosed source value: %v", err)
	}
	if _, ok := ErrorCodeOf(errors.New("ordinary error")); ok {
		t.Fatal("ordinary error was classified as a parser error")
	}
}

func TestNewRejectsUnsafeOrIncompleteLimits(t *testing.T) {
	valid := DefaultLimits()
	tests := []struct {
		name   string
		mutate func(*Limits)
	}{
		{name: "zero bytes", mutate: func(limits *Limits) { limits.MaxBytes = 0 }},
		{name: "excessive bytes", mutate: func(limits *Limits) { limits.MaxBytes = hardMaxBytes + 1 }},
		{name: "zero depth", mutate: func(limits *Limits) { limits.MaxJSONDepth = 0 }},
		{name: "excessive depth", mutate: func(limits *Limits) { limits.MaxJSONDepth = hardMaxJSONDepth + 1 }},
		{name: "zero members", mutate: func(limits *Limits) { limits.MaxObjectMembers = 0 }},
		{name: "runs exceed arrays", mutate: func(limits *Limits) { limits.MaxRuns = limits.MaxArrayElements + 1 }},
		{name: "zero warnings", mutate: func(limits *Limits) { limits.MaxWarnings = 0 }},
		{name: "zero warning fields", mutate: func(limits *Limits) { limits.MaxUnsupportedFieldsPerWarning = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := valid
			test.mutate(&limits)
			_, err := New(limits)
			assertErrorCode(t, err, ErrorInvalidConfiguration)
		})
	}
}

func FuzzParserNeverPanics(fuzz *testing.F) {
	fuzz.Add(readFuzzFixture(fuzz, "valid", "minimal.sarif.json"))
	fuzz.Add(readFuzzFixture(fuzz, "valid", "representative.sarif.json"))
	fuzz.Add(readFuzzFixture(fuzz, "invalid", "malformed-json.sarif.json"))
	limits := DefaultLimits()
	limits.MaxBytes = 8 << 10
	limits.MaxStringBytes = 4 << 10
	parser, err := New(limits)
	if err != nil {
		fuzz.Fatal(err)
	}
	fuzz.Fuzz(func(t *testing.T, source []byte) {
		document, err := parser.Parse(context.Background(), bytes.NewReader(source))
		if err != nil {
			if _, ok := ErrorCodeOf(err); !ok {
				t.Fatalf("unclassified parser error = %v", err)
			}
			return
		}
		if _, err := json.Marshal(document); err != nil {
			t.Fatalf("marshal parsed document: %v", err)
		}
	})
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func newTestParser(t *testing.T) *Parser {
	t.Helper()
	parser, err := New(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return parser
}

func readParserFixture(t *testing.T, parts ...string) []byte {
	t.Helper()
	path := filepath.Join(append([]string{fixtureRoot}, parts...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func readFuzzFixture(fuzz *testing.F, parts ...string) []byte {
	fuzz.Helper()
	path := filepath.Join(append([]string{fixtureRoot}, parts...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		fuzz.Fatal(err)
	}
	return data
}

func assertErrorCode(t *testing.T, err error, want ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s", want)
	}
	got, ok := ErrorCodeOf(err)
	if !ok || got != want {
		t.Fatalf("error = %v, code = %q/%t, want %q", err, got, ok, want)
	}
}

func strconvQuote(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
