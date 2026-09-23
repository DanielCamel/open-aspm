package sarif

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	hardMaxBytes       = 256 << 20
	hardMaxJSONDepth   = 256
	hardMaxCollection  = 1_000_000
	hardMaxStringBytes = 4 << 20
	hardMaxWarnings    = 100_000
)

// Parser decodes one explicitly bounded SARIF 2.1.0 document.
type Parser struct {
	limits Limits
}

// New validates finite parser limits and returns a SARIF 2.1.0 parser.
func New(limits Limits) (*Parser, error) {
	if limits.MaxBytes <= 0 || limits.MaxBytes > hardMaxBytes ||
		limits.MaxJSONDepth <= 0 || limits.MaxJSONDepth > hardMaxJSONDepth ||
		limits.MaxObjectMembers <= 0 || limits.MaxObjectMembers > hardMaxCollection ||
		limits.MaxArrayElements <= 0 || limits.MaxArrayElements > hardMaxCollection ||
		limits.MaxStringBytes <= 0 || limits.MaxStringBytes > hardMaxStringBytes ||
		limits.MaxRuns <= 0 || limits.MaxRuns > limits.MaxArrayElements ||
		limits.MaxRulesPerRun <= 0 || limits.MaxRulesPerRun > limits.MaxArrayElements ||
		limits.MaxResultsPerRun <= 0 || limits.MaxResultsPerRun > limits.MaxArrayElements ||
		limits.MaxInvocationsPerRun <= 0 || limits.MaxInvocationsPerRun > limits.MaxArrayElements ||
		limits.MaxLocationsPerResult <= 0 || limits.MaxLocationsPerResult > limits.MaxArrayElements ||
		limits.MaxFingerprintsPerResult <= 0 || limits.MaxFingerprintsPerResult > limits.MaxObjectMembers ||
		limits.MaxWarnings <= 0 || limits.MaxWarnings > hardMaxWarnings ||
		limits.MaxUnsupportedFieldsPerWarning <= 0 ||
		limits.MaxUnsupportedFieldsPerWarning > limits.MaxObjectMembers {
		return nil, newParseError(ErrorInvalidConfiguration, "")
	}
	return &Parser{limits: limits}, nil
}

// Parse reads one SARIF log. A scanner-reported failed invocation remains a
// successful parse result with ExecutionSuccessful false.
func (parser *Parser) Parse(ctx context.Context, source io.Reader) (Document, error) {
	if parser == nil || source == nil {
		return Document{}, newParseError(ErrorInvalidConfiguration, "")
	}
	if err := contextError(ctx); err != nil {
		return Document{}, err
	}
	limited := io.LimitReader(contextReader{ctx: ctx, reader: source}, parser.limits.MaxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		if contextError := contextError(ctx); contextError != nil {
			return Document{}, contextError
		}
		return Document{}, newParseError(ErrorInputRead, "")
	}
	if int64(len(data)) > parser.limits.MaxBytes {
		return Document{}, newParseError(ErrorInputTooLarge, "")
	}
	if !utf8.Valid(data) {
		return Document{}, newParseError(ErrorInvalidEncoding, "")
	}
	rootKind, err := validateJSON(ctx, data, parser.limits)
	if err != nil {
		return Document{}, err
	}
	if rootKind != jsonObject {
		return Document{}, newParseError(ErrorInvalidSARIF, "")
	}
	state := parseState{ctx: ctx, limits: parser.limits}
	document, err := state.parseDocument(data)
	if err != nil {
		return Document{}, err
	}
	document.Warnings = state.warnings
	document.WarningsTruncated = state.warningsTruncated
	return document, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	select {
	case <-reader.ctx.Done():
		return 0, reader.ctx.Err()
	default:
		return reader.reader.Read(buffer)
	}
}

type jsonKind uint8

const (
	jsonScalar jsonKind = iota
	jsonObject
	jsonArray
)

func validateJSON(ctx context.Context, data []byte, limits Limits) (jsonKind, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	kind, err := validateJSONValue(ctx, decoder, limits, 0)
	if err != nil {
		return jsonScalar, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return jsonScalar, newParseError(ErrorMalformedJSON, "")
	}
	return kind, nil
}

func validateJSONValue(
	ctx context.Context,
	decoder *json.Decoder,
	limits Limits,
	depth int,
) (jsonKind, error) {
	if err := contextError(ctx); err != nil {
		return jsonScalar, err
	}
	token, err := decoder.Token()
	if err != nil {
		return jsonScalar, newParseError(ErrorMalformedJSON, "")
	}
	switch value := token.(type) {
	case json.Delim:
		if value != '{' && value != '[' {
			return jsonScalar, newParseError(ErrorMalformedJSON, "")
		}
		containerDepth := depth + 1
		if containerDepth > limits.MaxJSONDepth {
			return jsonScalar, newParseError(ErrorLimitExceeded, "")
		}
		if value == '{' {
			seen := make(map[string]struct{})
			members := 0
			for decoder.More() {
				if err := contextError(ctx); err != nil {
					return jsonScalar, err
				}
				keyToken, err := decoder.Token()
				if err != nil {
					return jsonScalar, newParseError(ErrorMalformedJSON, "")
				}
				key, ok := keyToken.(string)
				if !ok || len(key) > limits.MaxStringBytes {
					return jsonScalar, newParseError(ErrorLimitExceeded, "")
				}
				members++
				if members > limits.MaxObjectMembers {
					return jsonScalar, newParseError(ErrorLimitExceeded, "")
				}
				if _, duplicate := seen[key]; duplicate {
					return jsonScalar, newParseError(ErrorMalformedJSON, "")
				}
				seen[key] = struct{}{}
				if _, err := validateJSONValue(ctx, decoder, limits, containerDepth); err != nil {
					return jsonScalar, err
				}
			}
			if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') {
				return jsonScalar, newParseError(ErrorMalformedJSON, "")
			}
			return jsonObject, nil
		}
		elements := 0
		for decoder.More() {
			elements++
			if elements > limits.MaxArrayElements {
				return jsonScalar, newParseError(ErrorLimitExceeded, "")
			}
			if _, err := validateJSONValue(ctx, decoder, limits, containerDepth); err != nil {
				return jsonScalar, err
			}
		}
		if closing, err := decoder.Token(); err != nil || closing != json.Delim(']') {
			return jsonScalar, newParseError(ErrorMalformedJSON, "")
		}
		return jsonArray, nil
	case string:
		if len(value) > limits.MaxStringBytes {
			return jsonScalar, newParseError(ErrorLimitExceeded, "")
		}
		return jsonScalar, nil
	case json.Number, bool, nil:
		return jsonScalar, nil
	default:
		return jsonScalar, newParseError(ErrorMalformedJSON, "")
	}
}

type parseState struct {
	ctx               context.Context
	limits            Limits
	warnings          []Warning
	warningsTruncated bool
}

type wireDocument struct {
	Schema  string            `json:"$schema"`
	Version string            `json:"version"`
	Runs    []json.RawMessage `json:"runs"`
}

func (state *parseState) parseDocument(data []byte) (Document, error) {
	var fields map[string]json.RawMessage
	var wire wireDocument
	if json.Unmarshal(data, &fields) != nil || json.Unmarshal(data, &wire) != nil {
		return Document{}, newParseError(ErrorInvalidSARIF, "")
	}
	if _, ok := fields["version"]; !ok || wire.Version == "" {
		return Document{}, newParseError(ErrorInvalidSARIF, "/version")
	}
	if wire.Version != FormatVersion {
		return Document{}, newParseError(ErrorUnsupportedVersion, "/version")
	}
	if _, ok := fields["runs"]; !ok {
		return Document{}, newParseError(ErrorInvalidSARIF, "/runs")
	}
	if len(wire.Runs) > state.limits.MaxRuns {
		return Document{}, newParseError(ErrorLimitExceeded, "/runs")
	}
	state.recordUnsupported(data, "", "$schema", "version", "runs")
	document := Document{
		Format: Format, FormatVersion: FormatVersion, SchemaURI: wire.Schema,
		Parser: ParserIdentity{Name: Format, Version: ParserVersion},
		Runs:   make([]Run, 0, len(wire.Runs)),
	}
	for index, rawRun := range wire.Runs {
		if err := contextError(state.ctx); err != nil {
			return Document{}, err
		}
		run, err := state.parseRun(rawRun, index)
		if err != nil {
			return Document{}, err
		}
		document.Runs = append(document.Runs, run)
	}
	return document, nil
}

type wireRun struct {
	Tool              json.RawMessage   `json:"tool"`
	AutomationDetails json.RawMessage   `json:"automationDetails"`
	Invocations       []json.RawMessage `json:"invocations"`
	Results           []json.RawMessage `json:"results"`
}

type wireTool struct {
	Driver json.RawMessage `json:"driver"`
}

type wireDriver struct {
	Name            string            `json:"name"`
	FullName        string            `json:"fullName"`
	Version         string            `json:"version"`
	SemanticVersion string            `json:"semanticVersion"`
	InformationURI  string            `json:"informationUri"`
	Rules           []json.RawMessage `json:"rules"`
}

func (state *parseState) parseRun(raw json.RawMessage, index int) (Run, error) {
	path := indexPath("/runs", index)
	var wire wireRun
	if json.Unmarshal(raw, &wire) != nil || len(wire.Tool) == 0 {
		return Run{}, newParseError(ErrorInvalidSARIF, path)
	}
	if len(wire.Invocations) > state.limits.MaxInvocationsPerRun {
		return Run{}, newParseError(ErrorLimitExceeded, path+"/invocations")
	}
	if len(wire.Results) > state.limits.MaxResultsPerRun {
		return Run{}, newParseError(ErrorLimitExceeded, path+"/results")
	}
	state.recordUnsupported(raw, path, "tool", "automationDetails", "invocations", "results")
	var toolWire wireTool
	if json.Unmarshal(wire.Tool, &toolWire) != nil || len(toolWire.Driver) == 0 {
		return Run{}, newParseError(ErrorInvalidSARIF, path+"/tool")
	}
	state.recordUnsupported(wire.Tool, path+"/tool", "driver")
	var driver wireDriver
	if json.Unmarshal(toolWire.Driver, &driver) != nil || strings.TrimSpace(driver.Name) == "" {
		return Run{}, newParseError(ErrorInvalidSARIF, path+"/tool/driver/name")
	}
	if len(driver.Rules) > state.limits.MaxRulesPerRun {
		return Run{}, newParseError(ErrorLimitExceeded, path+"/tool/driver/rules")
	}
	state.recordUnsupported(
		toolWire.Driver, path+"/tool/driver",
		"name", "fullName", "version", "semanticVersion", "informationUri", "rules",
	)
	run := Run{
		Index: index, SourcePointer: path,
		Tool: Tool{
			Name: driver.Name, FullName: driver.FullName, Version: driver.Version,
			SemanticVersion: driver.SemanticVersion, InformationURI: driver.InformationURI,
		},
		Rules:       make([]Rule, 0, len(driver.Rules)),
		Invocations: make([]Invocation, 0, len(wire.Invocations)),
		Results:     make([]Result, 0, len(wire.Results)),
	}
	if len(wire.AutomationDetails) != 0 && !bytes.Equal(wire.AutomationDetails, []byte("null")) {
		details, err := state.parseAutomationDetails(wire.AutomationDetails, path+"/automationDetails")
		if err != nil {
			return Run{}, err
		}
		run.AutomationDetails = &details
	}
	for ruleIndex, rawRule := range driver.Rules {
		rule, err := state.parseRule(rawRule, path+"/tool/driver/rules", ruleIndex)
		if err != nil {
			return Run{}, err
		}
		run.Rules = append(run.Rules, rule)
	}
	for invocationIndex, rawInvocation := range wire.Invocations {
		invocation, err := state.parseInvocation(rawInvocation, path+"/invocations", invocationIndex)
		if err != nil {
			return Run{}, err
		}
		run.Invocations = append(run.Invocations, invocation)
	}
	for resultIndex, rawResult := range wire.Results {
		result, err := state.parseResult(rawResult, path+"/results", resultIndex)
		if err != nil {
			return Run{}, err
		}
		run.Results = append(run.Results, result)
	}
	return run, nil
}

type wireAutomationDetails struct {
	ID              string `json:"id"`
	GUID            string `json:"guid"`
	CorrelationGUID string `json:"correlationGuid"`
}

func (state *parseState) parseAutomationDetails(
	raw json.RawMessage,
	path string,
) (AutomationDetails, error) {
	var wire wireAutomationDetails
	if json.Unmarshal(raw, &wire) != nil {
		return AutomationDetails{}, newParseError(ErrorInvalidSARIF, path)
	}
	state.recordUnsupported(raw, path, "id", "guid", "correlationGuid")
	return AutomationDetails{
		ID: wire.ID, GUID: wire.GUID, CorrelationGUID: wire.CorrelationGUID,
	}, nil
}

type wireInvocation struct {
	ExecutionSuccessful        *bool  `json:"executionSuccessful"`
	StartTimeUTC               string `json:"startTimeUtc"`
	EndTimeUTC                 string `json:"endTimeUtc"`
	ExitCode                   *int   `json:"exitCode"`
	ExitCodeDescription        string `json:"exitCodeDescription"`
	ProcessStartFailureMessage string `json:"processStartFailureMessage"`
}

func (state *parseState) parseInvocation(
	raw json.RawMessage,
	parentPath string,
	index int,
) (Invocation, error) {
	path := indexPath(parentPath, index)
	var wire wireInvocation
	if json.Unmarshal(raw, &wire) != nil || wire.ExecutionSuccessful == nil {
		return Invocation{}, newParseError(ErrorInvalidSARIF, path)
	}
	state.recordUnsupported(
		raw, path, "executionSuccessful", "startTimeUtc", "endTimeUtc", "exitCode",
		"exitCodeDescription", "processStartFailureMessage",
	)
	return Invocation{
		Index: index, SourcePointer: path, ExecutionSuccessful: *wire.ExecutionSuccessful,
		StartTimeUTC: wire.StartTimeUTC, EndTimeUTC: wire.EndTimeUTC, ExitCode: wire.ExitCode,
		ExitCodeDescription: wire.ExitCodeDescription,
		StartFailureMessage: wire.ProcessStartFailureMessage,
	}, nil
}

type wireRule struct {
	ID                   string          `json:"id"`
	Name                 string          `json:"name"`
	ShortDescription     json.RawMessage `json:"shortDescription"`
	FullDescription      json.RawMessage `json:"fullDescription"`
	Help                 json.RawMessage `json:"help"`
	DefaultConfiguration json.RawMessage `json:"defaultConfiguration"`
}

type wireReportingConfiguration struct {
	Level string `json:"level"`
}

func (state *parseState) parseRule(raw json.RawMessage, parentPath string, index int) (Rule, error) {
	path := indexPath(parentPath, index)
	var wire wireRule
	if json.Unmarshal(raw, &wire) != nil || strings.TrimSpace(wire.ID) == "" {
		return Rule{}, newParseError(ErrorInvalidSARIF, path)
	}
	var configuration wireReportingConfiguration
	if len(wire.DefaultConfiguration) != 0 && !bytes.Equal(wire.DefaultConfiguration, []byte("null")) {
		if json.Unmarshal(wire.DefaultConfiguration, &configuration) != nil ||
			(configuration.Level != "" && !validLevel(configuration.Level)) {
			return Rule{}, newParseError(ErrorInvalidSARIF, path+"/defaultConfiguration")
		}
		state.recordUnsupported(wire.DefaultConfiguration, path+"/defaultConfiguration", "level")
	}
	state.recordUnsupported(
		raw, path, "id", "name", "shortDescription", "fullDescription", "help",
		"defaultConfiguration",
	)
	rule := Rule{
		Index: index, SourcePointer: path, ID: wire.ID, Name: wire.Name,
		DefaultLevel: configuration.Level,
	}
	var err error
	if rule.ShortDescription, err = state.parseOptionalMultiformatMessage(
		wire.ShortDescription, path+"/shortDescription",
	); err != nil {
		return Rule{}, err
	}
	if rule.FullDescription, err = state.parseOptionalMultiformatMessage(
		wire.FullDescription, path+"/fullDescription",
	); err != nil {
		return Rule{}, err
	}
	if rule.Help, err = state.parseOptionalMultiformatMessage(wire.Help, path+"/help"); err != nil {
		return Rule{}, err
	}
	return rule, nil
}

type wireResult struct {
	GUID                string            `json:"guid"`
	CorrelationGUID     string            `json:"correlationGuid"`
	RuleID              string            `json:"ruleId"`
	RuleIndex           *int              `json:"ruleIndex"`
	Level               string            `json:"level"`
	Kind                string            `json:"kind"`
	BaselineState       string            `json:"baselineState"`
	Message             json.RawMessage   `json:"message"`
	Locations           []json.RawMessage `json:"locations"`
	Fingerprints        map[string]string `json:"fingerprints"`
	PartialFingerprints map[string]string `json:"partialFingerprints"`
}

func (state *parseState) parseResult(
	raw json.RawMessage,
	parentPath string,
	index int,
) (Result, error) {
	path := indexPath(parentPath, index)
	var wire wireResult
	if json.Unmarshal(raw, &wire) != nil || len(wire.Message) == 0 {
		return Result{}, newParseError(ErrorInvalidSARIF, path)
	}
	if wire.RuleIndex != nil && *wire.RuleIndex < 0 {
		return Result{}, newParseError(ErrorInvalidSARIF, path+"/ruleIndex")
	}
	if wire.Level != "" && !validLevel(wire.Level) {
		return Result{}, newParseError(ErrorInvalidSARIF, path+"/level")
	}
	if wire.Kind != "" && !validKind(wire.Kind) {
		return Result{}, newParseError(ErrorInvalidSARIF, path+"/kind")
	}
	if wire.BaselineState != "" && !validBaselineState(wire.BaselineState) {
		return Result{}, newParseError(ErrorInvalidSARIF, path+"/baselineState")
	}
	if len(wire.Locations) > state.limits.MaxLocationsPerResult {
		return Result{}, newParseError(ErrorLimitExceeded, path+"/locations")
	}
	if len(wire.Fingerprints)+len(wire.PartialFingerprints) > state.limits.MaxFingerprintsPerResult {
		return Result{}, newParseError(ErrorLimitExceeded, path+"/fingerprints")
	}
	state.recordUnsupported(
		raw, path, "guid", "correlationGuid", "ruleId", "ruleIndex", "level", "kind",
		"baselineState", "message", "locations", "fingerprints", "partialFingerprints",
	)
	message, err := state.parseMessage(wire.Message, path+"/message")
	if err != nil {
		return Result{}, err
	}
	result := Result{
		Index: index, SourcePointer: path, GUID: wire.GUID, CorrelationGUID: wire.CorrelationGUID,
		RuleID: wire.RuleID, RuleIndex: wire.RuleIndex, Level: wire.Level, Kind: wire.Kind,
		BaselineState: wire.BaselineState, Message: message,
		Locations:           make([]Location, 0, len(wire.Locations)),
		Fingerprints:        namedValues(wire.Fingerprints),
		PartialFingerprints: namedValues(wire.PartialFingerprints),
	}
	for locationIndex, rawLocation := range wire.Locations {
		location, err := state.parseLocation(rawLocation, path+"/locations", locationIndex)
		if err != nil {
			return Result{}, err
		}
		result.Locations = append(result.Locations, location)
	}
	return result, nil
}

type wireMessage struct {
	ID        string   `json:"id"`
	Text      string   `json:"text"`
	Markdown  string   `json:"markdown"`
	Arguments []string `json:"arguments"`
}

func (state *parseState) parseOptionalMultiformatMessage(
	raw json.RawMessage,
	path string,
) (*Message, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	var wire struct {
		Text     string `json:"text"`
		Markdown string `json:"markdown"`
	}
	if json.Unmarshal(raw, &wire) != nil || wire.Text == "" {
		return nil, newParseError(ErrorInvalidSARIF, path)
	}
	state.recordUnsupported(raw, path, "text", "markdown")
	message := Message{Text: wire.Text, Markdown: wire.Markdown}
	return &message, nil
}

func (state *parseState) parseMessage(raw json.RawMessage, path string) (Message, error) {
	var wire wireMessage
	if json.Unmarshal(raw, &wire) != nil || (wire.ID == "" && wire.Text == "") {
		return Message{}, newParseError(ErrorInvalidSARIF, path)
	}
	state.recordUnsupported(raw, path, "id", "text", "markdown", "arguments")
	return Message{
		ID: wire.ID, Text: wire.Text, Markdown: wire.Markdown,
		Arguments: append([]string(nil), wire.Arguments...),
	}, nil
}

type wireLocation struct {
	PhysicalLocation json.RawMessage `json:"physicalLocation"`
}

type wirePhysicalLocation struct {
	ArtifactLocation json.RawMessage `json:"artifactLocation"`
	Region           json.RawMessage `json:"region"`
}

type wireArtifactLocation struct {
	URI       string `json:"uri"`
	URIBaseID string `json:"uriBaseId"`
	Index     *int   `json:"index"`
}

type wireRegion struct {
	StartLine   *int `json:"startLine"`
	StartColumn *int `json:"startColumn"`
	EndLine     *int `json:"endLine"`
	EndColumn   *int `json:"endColumn"`
}

func (state *parseState) parseLocation(
	raw json.RawMessage,
	parentPath string,
	index int,
) (Location, error) {
	path := indexPath(parentPath, index)
	var wire wireLocation
	if json.Unmarshal(raw, &wire) != nil {
		return Location{}, newParseError(ErrorInvalidSARIF, path)
	}
	state.recordUnsupported(raw, path, "physicalLocation")
	location := Location{Index: index, SourcePointer: path}
	if len(wire.PhysicalLocation) == 0 || bytes.Equal(wire.PhysicalLocation, []byte("null")) {
		return location, nil
	}
	var physical wirePhysicalLocation
	if json.Unmarshal(wire.PhysicalLocation, &physical) != nil {
		return Location{}, newParseError(ErrorInvalidSARIF, path+"/physicalLocation")
	}
	state.recordUnsupported(wire.PhysicalLocation, path+"/physicalLocation", "artifactLocation", "region")
	if len(physical.ArtifactLocation) != 0 && !bytes.Equal(physical.ArtifactLocation, []byte("null")) {
		var artifact wireArtifactLocation
		if json.Unmarshal(physical.ArtifactLocation, &artifact) != nil ||
			(artifact.Index != nil && *artifact.Index < 0) {
			return Location{}, newParseError(ErrorInvalidSARIF, path+"/physicalLocation/artifactLocation")
		}
		state.recordUnsupported(
			physical.ArtifactLocation, path+"/physicalLocation/artifactLocation",
			"uri", "uriBaseId", "index",
		)
		location.URI = artifact.URI
		location.URIBaseID = artifact.URIBaseID
		location.ArtifactIndex = artifact.Index
	}
	if len(physical.Region) != 0 && !bytes.Equal(physical.Region, []byte("null")) {
		var region wireRegion
		if json.Unmarshal(physical.Region, &region) != nil || !validRegion(region) {
			return Location{}, newParseError(ErrorInvalidSARIF, path+"/physicalLocation/region")
		}
		state.recordUnsupported(
			physical.Region, path+"/physicalLocation/region",
			"startLine", "startColumn", "endLine", "endColumn",
		)
		location.Region = &Region{
			StartLine: region.StartLine, StartColumn: region.StartColumn,
			EndLine: region.EndLine, EndColumn: region.EndColumn,
		}
	}
	return location, nil
}

func (state *parseState) recordUnsupported(raw json.RawMessage, path string, known ...string) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return
	}
	for _, name := range known {
		delete(fields, name)
	}
	if len(fields) == 0 {
		return
	}
	if len(state.warnings) >= state.limits.MaxWarnings {
		state.warningsTruncated = true
		return
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	warning := Warning{
		Code: WarningUnsupportedFields, Path: path, FieldCount: len(names),
	}
	if len(names) > state.limits.MaxUnsupportedFieldsPerWarning {
		warning.Fields = append([]string(nil), names[:state.limits.MaxUnsupportedFieldsPerWarning]...)
		warning.FieldsTruncated = true
	} else {
		warning.Fields = names
	}
	state.warnings = append(state.warnings, warning)
}

func namedValues(values map[string]string) []NamedValue {
	if len(values) == 0 {
		return nil
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]NamedValue, 0, len(names))
	for _, name := range names {
		result = append(result, NamedValue{Name: name, Value: values[name]})
	}
	return result
}

func validLevel(value string) bool {
	return value == "none" || value == "note" || value == "warning" || value == "error"
}

func validKind(value string) bool {
	switch value {
	case "notApplicable", "pass", "fail", "review", "open", "informational":
		return true
	default:
		return false
	}
}

func validBaselineState(value string) bool {
	return value == "new" || value == "unchanged" || value == "updated" || value == "absent"
}

func validRegion(region wireRegion) bool {
	for _, value := range []*int{region.StartLine, region.StartColumn, region.EndLine, region.EndColumn} {
		if value != nil && *value < 1 {
			return false
		}
	}
	if region.StartLine != nil && region.EndLine != nil && *region.EndLine < *region.StartLine {
		return false
	}
	if region.StartLine != nil && region.EndLine != nil && *region.StartLine == *region.EndLine &&
		region.StartColumn != nil && region.EndColumn != nil && *region.EndColumn < *region.StartColumn {
		return false
	}
	return true
}

func indexPath(parent string, index int) string {
	return parent + "/" + strconv.Itoa(index)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return newParseError(ErrorInvalidConfiguration, "")
	}
	select {
	case <-ctx.Done():
		return newParseError(ErrorCancelled, "")
	default:
		return nil
	}
}

func newParseError(code ErrorCode, path string) error {
	return &ParseError{code: code, path: path}
}
