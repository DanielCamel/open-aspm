// Package importjob orchestrates the import.process application workflow while
// keeping authoritative writes behind their owning module interfaces.
package importjob

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/spectremi/open-aspm/internal/blobstore"
	"github.com/spectremi/open-aspm/internal/finding"
	"github.com/spectremi/open-aspm/internal/ingestion"
	"github.com/spectremi/open-aspm/internal/jobqueue"
	"github.com/spectremi/open-aspm/internal/parsing/sarif"
)

const terminalPersistenceTimeout = 5 * time.Second

type processingStore interface {
	ResolveProcessingTarget(context.Context, ingestion.ProcessingIdentity) (ingestion.ProcessingTarget, error)
	BeginProcessing(context.Context, ingestion.ProcessingIdentity, time.Time) (ingestion.ProcessingSource, error)
	RecordParseOutput(context.Context, ingestion.ParseOutputSpec) error
	CompleteProcessing(context.Context, ingestion.ProcessingIdentity, ingestion.ProcessingResult, time.Time) error
	FailProcessing(context.Context, ingestion.ProcessingIdentity, ingestion.ProcessingFailure, time.Time) error
}

type scanRecorder interface {
	RecordScan(context.Context, ingestion.ScanSpec) (ingestion.StoredScan, error)
}

type observationStore interface {
	RecordObservation(context.Context, finding.ObservationSpec) (finding.StoredObservation, error)
	RecordNormalization(context.Context, finding.NormalizationSpec) (finding.StoredNormalization, error)
}

type observationNormalizer interface {
	Normalize(finding.ObservationSpec, time.Time) (finding.NormalizationSpec, error)
}

type blobReader interface {
	Open(context.Context, blobstore.Key) (io.ReadCloser, blobstore.Metadata, error)
}

type sarifParser interface {
	Parse(context.Context, io.Reader) (sarif.Document, error)
}

type Config struct {
	StorageBackend    string
	ProcessingTimeout time.Duration
	RetryMaximumAge   time.Duration
}

// Processor is the idempotent handler for one import.process job.
type Processor struct {
	store        processingStore
	scans        scanRecorder
	observations observationStore
	blobs        blobReader
	parser       sarifParser
	normalizer   observationNormalizer
	authorizer   ingestion.Authorizer
	config       Config
	now          func() time.Time
	random       func([]byte) error
}

func New(
	store processingStore,
	scans scanRecorder,
	observations observationStore,
	blobs blobReader,
	parser sarifParser,
	normalizer observationNormalizer,
	authorizer ingestion.Authorizer,
	config Config,
) (*Processor, error) {
	if store == nil || scans == nil || observations == nil || blobs == nil || parser == nil || normalizer == nil ||
		authorizer == nil ||
		config.StorageBackend == "" || config.ProcessingTimeout <= 0 || config.RetryMaximumAge <= 0 {
		return nil, errors.New("invalid import processor configuration")
	}
	return &Processor{
		store: store, scans: scans, observations: observations, blobs: blobs,
		parser: parser, normalizer: normalizer, authorizer: authorizer, config: config,
		now: time.Now,
		random: func(buffer []byte) error {
			_, err := rand.Read(buffer)
			return err
		},
	}, nil
}

// Handle validates the internal queue contract, reauthorizes the delayed work,
// verifies immutable evidence, and persists deterministic parsed and normalized output.
func (processor *Processor) Handle(ctx context.Context, job jobqueue.Job) jobqueue.Outcome {
	identity, err := decodeJob(job)
	if err != nil {
		return jobqueue.Outcome{Kind: jobqueue.OutcomePermanent, Code: "invalid-import-job"}
	}
	target, err := processor.store.ResolveProcessingTarget(ctx, identity)
	if err != nil {
		if errors.Is(err, ingestion.ErrProcessingNotFound) || errors.Is(err, ingestion.ErrInvalid) {
			return jobqueue.Outcome{Kind: jobqueue.OutcomePermanent, Code: "processing-target-not-found"}
		}
		return processor.retry(job, identity, "database-unavailable", "Import processing storage is unavailable")
	}
	if err := processor.authorizer.Authorize(ctx, ingestion.AuthorizationRequest{
		PrincipalID: identity.PrincipalID, WorkspaceID: identity.WorkspaceID,
		ApplicationID: target.ApplicationID, Capability: identity.JobCapability,
	}); err != nil {
		if errors.Is(err, ingestion.ErrForbidden) {
			return processor.permanent(identity, "authorization-denied", "Import processing authorization was denied")
		}
		return processor.retry(job, identity, "authorization-unavailable", "Import processing authorization is unavailable")
	}
	if terminal := terminalOutcome(target.State, target.FailureCode); terminal != nil {
		return *terminal
	}

	startedAt := processor.timestamp(time.Time{})
	source, err := processor.store.BeginProcessing(ctx, identity, startedAt)
	if err != nil {
		if errors.Is(err, ingestion.ErrProcessingNotFound) || errors.Is(err, ingestion.ErrProcessingConflict) ||
			errors.Is(err, ingestion.ErrInvalid) {
			return processor.permanent(identity, "processing-state-conflict", "Import processing state is inconsistent")
		}
		return processor.retry(job, identity, "database-unavailable", "Import processing storage is unavailable")
	}
	if terminal := terminalOutcome(source.State, source.FailureCode); terminal != nil {
		return *terminal
	}
	if source.ReportFormat.Name != sarif.Format ||
		(source.ReportFormat.Version != "" && source.ReportFormat.Version != sarif.FormatVersion) {
		return processor.permanent(identity, "unsupported-report-format", "The report format is not supported")
	}
	if source.StorageBackend != processor.config.StorageBackend {
		return processor.retry(job, identity, "storage-backend-unavailable", "The evidence storage backend is unavailable")
	}
	if source.MaxBytes <= 0 || source.SizeBytes < 0 || source.SizeBytes > source.MaxBytes ||
		!blobstore.ValidSHA256(source.SHA256) {
		return processor.permanent(identity, "evidence-integrity-failed", "Committed evidence metadata is invalid")
	}
	key, err := blobstore.ParseKey(source.StorageKey)
	if err != nil {
		return processor.permanent(identity, "evidence-reference-invalid", "The evidence reference is invalid")
	}

	processingCtx, cancel := context.WithTimeout(ctx, processor.config.ProcessingTimeout)
	defer cancel()
	stream, metadata, err := processor.blobs.Open(processingCtx, key)
	if err != nil {
		return processor.blobFailure(job, identity, err)
	}
	if !matchingEvidence(source, metadata) {
		_ = stream.Close()
		return processor.permanent(identity, "evidence-integrity-failed", "Stored evidence did not match committed metadata")
	}
	tracked := &trackingReader{source: stream}
	document, parseErr := processor.parser.Parse(processingCtx, tracked)
	closeErr := stream.Close()
	if parseErr != nil {
		if errors.Is(processingCtx.Err(), context.DeadlineExceeded) {
			return processor.retry(job, identity, "processing-timeout", "Import processing exceeded its time limit")
		}
		return processor.parseFailure(job, identity, parseErr, tracked.err)
	}
	if closeErr != nil {
		return processor.retry(job, identity, "evidence-read-failed", "Stored evidence could not be read")
	}

	recordedAt := processor.timestamp(source.ReceivedAt)
	if err := processor.store.RecordParseOutput(ctx, mapParseOutput(source, document, recordedAt)); err != nil {
		if errors.Is(err, ingestion.ErrParseOutputConflict) {
			return processor.permanent(identity, "parser-output-conflict", "Parser output conflicts with retained history")
		}
		if errors.Is(err, ingestion.ErrInvalid) {
			return processor.permanent(identity, "parser-output-invalid", "Parser output is outside supported limits")
		}
		return processor.retry(job, identity, "database-unavailable", "Parser output could not be stored")
	}
	for _, run := range document.Runs {
		scanID, err := processor.identifier("scan_")
		if err != nil {
			return processor.retry(job, identity, "identifier-generation-failed", "A secure record identifier could not be generated")
		}
		storedScan, err := processor.scans.RecordScan(ctx, mapScan(source, document, run, scanID, recordedAt))
		if err != nil {
			if errors.Is(err, ingestion.ErrScanConflict) {
				return processor.permanent(identity, "scan-output-conflict", "Scan output conflicts with retained history")
			}
			if errors.Is(err, ingestion.ErrInvalid) {
				return processor.permanent(identity, "scan-output-invalid", "Scan output is outside supported limits")
			}
			return processor.retry(job, identity, "database-unavailable", "Scan output could not be stored")
		}
		for _, result := range run.Results {
			observationID, err := processor.identifier("obs_")
			if err != nil {
				return processor.retry(job, identity, "identifier-generation-failed", "A secure record identifier could not be generated")
			}
			observation := mapObservation(source, document, storedScan.ID, result, observationID, recordedAt)
			storedObservation, err := processor.observations.RecordObservation(ctx, observation)
			if err != nil {
				if errors.Is(err, finding.ErrObservationConflict) {
					return processor.permanent(identity, "observation-output-conflict", "Observation output conflicts with retained history")
				}
				if errors.Is(err, finding.ErrInvalid) {
					return processor.permanent(identity, "observation-output-invalid", "Observation output is outside supported limits")
				}
				return processor.retry(job, identity, "database-unavailable", "Observation output could not be stored")
			}
			observation.ID = storedObservation.ID
			normalized, err := processor.normalizer.Normalize(
				observation,
				processor.timestamp(storedObservation.RecordedAt),
			)
			if err != nil {
				return processor.permanent(identity, "normalization-output-invalid", "Observation normalization failed")
			}
			if _, err := processor.observations.RecordNormalization(ctx, normalized); err != nil {
				switch {
				case errors.Is(err, finding.ErrNormalizationConflict):
					return processor.permanent(identity, "normalization-output-conflict", "Normalization output conflicts with retained history")
				case errors.Is(err, finding.ErrNormalizationInvalid), errors.Is(err, finding.ErrObservationNotFound):
					return processor.permanent(identity, "normalization-output-invalid", "Normalization output is outside supported limits")
				default:
					return processor.retry(job, identity, "database-unavailable", "Normalization output could not be stored")
				}
			}
		}
	}
	if err := processor.store.CompleteProcessing(
		ctx, identity, ingestion.ProcessingResult{ScansCreated: len(document.Runs)}, processor.timestamp(source.ReceivedAt),
	); err != nil {
		if errors.Is(err, ingestion.ErrProcessingConflict) || errors.Is(err, ingestion.ErrProcessingNotFound) ||
			errors.Is(err, ingestion.ErrInvalid) {
			return processor.permanent(identity, "processing-state-conflict", "Import processing state is inconsistent")
		}
		return processor.retry(job, identity, "database-unavailable", "Import processing result could not be stored")
	}
	return jobqueue.Outcome{Kind: jobqueue.OutcomeSucceeded}
}

func decodeJob(job jobqueue.Job) (ingestion.ProcessingIdentity, error) {
	if job.Kind != ingestion.ImportProcessJobKind || job.SchemaVersion != ingestion.ImportProcessJobSchema ||
		job.SystemCapability != ingestion.CapabilityImportsProcess || job.WorkspaceID == "" ||
		job.OperationID == "" || job.InitiatingPrincipalID == "" {
		return ingestion.ProcessingIdentity{}, errors.New("invalid job envelope")
	}
	var payload struct {
		ImportID string `json:"import_id"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(job.Payload)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || payload.ImportID == "" || decoder.Decode(&struct{}{}) != io.EOF {
		return ingestion.ProcessingIdentity{}, errors.New("invalid job payload")
	}
	return ingestion.ProcessingIdentity{
		WorkspaceID: job.WorkspaceID, OperationID: job.OperationID, ImportID: payload.ImportID,
		PrincipalID: job.InitiatingPrincipalID, JobCapability: job.SystemCapability,
	}, nil
}

func terminalOutcome(state ingestion.OperationState, failureCode string) *jobqueue.Outcome {
	var outcome jobqueue.Outcome
	switch state {
	case ingestion.OperationSucceeded:
		outcome.Kind = jobqueue.OutcomeSucceeded
	case ingestion.OperationFailed:
		outcome.Kind = jobqueue.OutcomePermanent
		outcome.Code = failureCode
		if outcome.Code == "" {
			outcome.Code = "processing-failed"
		}
	case ingestion.OperationCancelled:
		outcome.Kind = jobqueue.OutcomeCancelled
	default:
		return nil
	}
	return &outcome
}

func (processor *Processor) retry(
	job jobqueue.Job,
	identity ingestion.ProcessingIdentity,
	code, title string,
) jobqueue.Outcome {
	now := processor.now().UTC()
	exhausted := job.MaxAttempts > 0 && job.AttemptCount >= job.MaxAttempts
	tooOld := !job.CreatedAt.IsZero() && now.Sub(job.CreatedAt) >= processor.config.RetryMaximumAge
	if exhausted || tooOld {
		return processor.permanent(identity, code, title)
	}
	return jobqueue.Outcome{Kind: jobqueue.OutcomeRetryable, Code: code}
}

func (processor *Processor) permanent(
	identity ingestion.ProcessingIdentity,
	code, title string,
) jobqueue.Outcome {
	failureCtx, cancel := context.WithTimeout(context.Background(), terminalPersistenceTimeout)
	defer cancel()
	err := processor.store.FailProcessing(
		failureCtx, identity,
		ingestion.ProcessingFailure{Code: code, Title: title}, processor.timestamp(time.Time{}),
	)
	if err != nil && !errors.Is(err, ingestion.ErrProcessingConflict) &&
		!errors.Is(err, ingestion.ErrProcessingNotFound) {
		return jobqueue.Outcome{Kind: jobqueue.OutcomeRetryable, Code: "state-persistence-failed"}
	}
	return jobqueue.Outcome{Kind: jobqueue.OutcomePermanent, Code: code}
}

func (processor *Processor) blobFailure(
	job jobqueue.Job,
	identity ingestion.ProcessingIdentity,
	err error,
) jobqueue.Outcome {
	switch {
	case errors.Is(err, context.Canceled):
		return jobqueue.Outcome{Kind: jobqueue.OutcomeCancelled}
	case errors.Is(err, blobstore.ErrIntegrity), errors.Is(err, blobstore.ErrInvalidKey):
		return processor.permanent(identity, "evidence-integrity-failed", "Stored evidence failed integrity verification")
	default:
		return processor.retry(job, identity, "evidence-unavailable", "Stored evidence is temporarily unavailable")
	}
}

func (processor *Processor) parseFailure(
	job jobqueue.Job,
	identity ingestion.ProcessingIdentity,
	parseErr, sourceErr error,
) jobqueue.Outcome {
	code, ok := sarif.ErrorCodeOf(parseErr)
	if !ok {
		return processor.retry(job, identity, "parser-failed", "The report parser failed")
	}
	switch code {
	case sarif.ErrorCancelled:
		return jobqueue.Outcome{Kind: jobqueue.OutcomeCancelled}
	case sarif.ErrorInputRead:
		if errors.Is(sourceErr, blobstore.ErrIntegrity) {
			return processor.permanent(identity, "evidence-integrity-failed", "Stored evidence failed integrity verification")
		}
		return processor.retry(job, identity, "evidence-read-failed", "Stored evidence could not be read")
	default:
		safeCode := "sarif-" + strings.ReplaceAll(string(code), "_", "-")
		return processor.permanent(identity, safeCode, "The SARIF report could not be processed")
	}
}

func matchingEvidence(source ingestion.ProcessingSource, metadata blobstore.Metadata) bool {
	return metadata.Size == source.SizeBytes && metadata.SHA256 == source.SHA256 &&
		metadata.Version == source.StorageVersion && metadata.BackendVersion == source.BackendVersion
}

func (processor *Processor) identifier(prefix string) (string, error) {
	buffer := make([]byte, 20)
	if err := processor.random(buffer); err != nil {
		return "", err
	}
	return prefix + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(buffer)), nil
}

func (processor *Processor) timestamp(notBefore time.Time) time.Time {
	value := processor.now().UTC().Truncate(time.Microsecond)
	if !notBefore.IsZero() && value.Before(notBefore) {
		return notBefore.UTC().Truncate(time.Microsecond)
	}
	return value
}

type trackingReader struct {
	source io.Reader
	err    error
}

func (reader *trackingReader) Read(buffer []byte) (int, error) {
	count, err := reader.source.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		reader.err = err
	}
	return count, err
}

func mapParseOutput(
	source ingestion.ProcessingSource,
	document sarif.Document,
	recordedAt time.Time,
) ingestion.ParseOutputSpec {
	warnings := make([]ingestion.ParseWarning, len(document.Warnings))
	for index, warning := range document.Warnings {
		warnings[index] = ingestion.ParseWarning{
			Code: string(warning.Code), SourcePointer: warning.Path, FieldCount: warning.FieldCount,
			Fields: append([]string(nil), warning.Fields...), FieldsTruncated: warning.FieldsTruncated,
		}
	}
	return ingestion.ParseOutputSpec{
		WorkspaceID: source.WorkspaceID, ImportID: source.ImportID, RawArtifactID: source.RawArtifactID,
		FormatName: document.Format, FormatVersion: document.FormatVersion,
		ParserName: document.Parser.Name, ParserVersion: document.Parser.Version,
		Warnings: warnings, WarningsTruncated: document.WarningsTruncated, RecordedAt: recordedAt,
	}
}

func mapScan(
	source ingestion.ProcessingSource,
	document sarif.Document,
	run sarif.Run,
	id string,
	recordedAt time.Time,
) ingestion.ScanSpec {
	result := ingestion.ScanResultUnknown
	if len(run.Invocations) > 0 {
		result = ingestion.ScanResultSucceeded
		for _, invocation := range run.Invocations {
			if !invocation.ExecutionSuccessful {
				result = ingestion.ScanResultFailed
				break
			}
		}
	}
	var startedAt, endedAt *time.Time
	if len(run.Invocations) == 1 {
		startedAt = parseSourceTime(run.Invocations[0].StartTimeUTC)
		endedAt = parseSourceTime(run.Invocations[0].EndTimeUTC)
		if startedAt != nil && endedAt != nil && endedAt.Before(*startedAt) {
			startedAt = nil
			endedAt = nil
		}
	}
	spec := ingestion.ScanSpec{
		ID: id, WorkspaceID: source.WorkspaceID, ImportID: source.ImportID,
		ApplicationID: source.ApplicationID, RawArtifactID: source.RawArtifactID,
		SourceRunIndex: run.Index, Result: result, Completeness: ingestion.ScanCompletenessUnknown,
		ScannerName: run.Tool.Name, ScannerFullName: run.Tool.FullName,
		ScannerVersion: run.Tool.Version, ScannerSemanticVersion: run.Tool.SemanticVersion,
		ParserName: document.Parser.Name, ParserVersion: document.Parser.Version,
		SourcePointer: run.SourcePointer, SourceStartedAt: startedAt, SourceEndedAt: endedAt,
		RecordedAt: recordedAt,
		Scope:      ingestion.ScanScope{SchemaVersion: 1, ScannerFamily: "unknown", AnalysisKind: "unknown"},
	}
	if run.AutomationDetails != nil {
		spec.AutomationID = run.AutomationDetails.ID
		spec.AutomationGUID = run.AutomationDetails.GUID
		spec.AutomationCorrelationGUID = run.AutomationDetails.CorrelationGUID
	}
	return spec
}

func mapObservation(
	source ingestion.ProcessingSource,
	document sarif.Document,
	scanID string,
	result sarif.Result,
	id string,
	recordedAt time.Time,
) finding.ObservationSpec {
	locations := make([]finding.Location, len(result.Locations))
	for index, location := range result.Locations {
		mapped := finding.Location{
			Ordinal: location.Index, SourcePointer: location.SourcePointer, URI: location.URI,
			URIBaseID: location.URIBaseID, ArtifactIndex: location.ArtifactIndex,
		}
		if location.Region != nil {
			mapped.StartLine = location.Region.StartLine
			mapped.StartColumn = location.Region.StartColumn
			mapped.EndLine = location.Region.EndLine
			mapped.EndColumn = location.Region.EndColumn
		}
		locations[index] = mapped
	}
	fingerprints := make([]finding.SourceFingerprint, 0, len(result.Fingerprints)+len(result.PartialFingerprints))
	for _, value := range result.Fingerprints {
		fingerprints = append(fingerprints, finding.SourceFingerprint{
			Kind: finding.FingerprintComplete, Name: value.Name, Value: value.Value,
		})
	}
	for _, value := range result.PartialFingerprints {
		fingerprints = append(fingerprints, finding.SourceFingerprint{
			Kind: finding.FingerprintPartial, Name: value.Name, Value: value.Value,
		})
	}
	return finding.ObservationSpec{
		ID: id, WorkspaceID: source.WorkspaceID, ScanID: scanID, RawArtifactID: source.RawArtifactID,
		SourceResultIndex: result.Index, ParserName: document.Parser.Name,
		ParserVersion: document.Parser.Version, SourcePointer: result.SourcePointer,
		SourceGUID: result.GUID, SourceCorrelationGUID: result.CorrelationGUID,
		SourceRuleID: result.RuleID, SourceRuleIndex: result.RuleIndex, SourceSeverity: result.Level,
		SourceKind: result.Kind, SourceBaselineState: result.BaselineState,
		MessageID: result.Message.ID, MessageText: result.Message.Text,
		MessageMarkdown:  result.Message.Markdown,
		MessageArguments: append([]string(nil), result.Message.Arguments...),
		Locations:        locations, Fingerprints: fingerprints, RecordedAt: recordedAt,
	}
}

func parseSourceTime(value string) *time.Time {
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil
	}
	parsed = parsed.UTC().Round(0)
	return &parsed
}
