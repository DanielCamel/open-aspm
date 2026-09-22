package testfixtures_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	sarifVersion          = "2.1.0"
	fixtureSizeLimit      = 256 << 10
	oversizedTestBoundary = 1 << 10
	deepNestingBoundary   = 16
)

var sarifRoot = filepath.Join("..", "..", "testdata", "sarif")

type sarifLog struct {
	Schema  string `json:"$schema"`
	Version string `json:"version"`
	Runs    []struct {
		Tool struct {
			Driver struct {
				Name  string `json:"name"`
				Rules []struct {
					ID string `json:"id"`
				} `json:"rules"`
			} `json:"driver"`
		} `json:"tool"`
		Results []struct {
			RuleID              string            `json:"ruleId"`
			Locations           []json.RawMessage `json:"locations"`
			Fingerprints        map[string]string `json:"fingerprints"`
			PartialFingerprints map[string]string `json:"partialFingerprints"`
		} `json:"results"`
	} `json:"runs"`
}

func TestValidSARIF21Fixtures(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(sarifRoot, "valid", "*.sarif.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("valid fixture count = %d, want 2", len(paths))
	}

	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data := readFixture(t, path)
			var log sarifLog
			if err := json.Unmarshal(data, &log); err != nil {
				t.Fatalf("decode valid fixture: %v", err)
			}
			if log.Version != sarifVersion {
				t.Errorf("version = %q, want %q", log.Version, sarifVersion)
			}
			if log.Schema == "" {
				t.Error("$schema is empty")
			}
			if len(log.Runs) == 0 {
				t.Fatal("runs is empty")
			}
			for i, run := range log.Runs {
				if run.Tool.Driver.Name == "" {
					t.Errorf("runs[%d].tool.driver.name is empty", i)
				}
			}
		})
	}
}

func TestRepresentativeFixtureCoverage(t *testing.T) {
	data := readFixture(t, filepath.Join(sarifRoot, "valid", "representative.sarif.json"))
	var log sarifLog
	if err := json.Unmarshal(data, &log); err != nil {
		t.Fatal(err)
	}
	if len(log.Runs) < 2 {
		t.Fatalf("runs = %d, want at least 2", len(log.Runs))
	}

	rules, locations, fingerprints, partialFingerprints := 0, 0, 0, 0
	resultWithoutOptionalFields := false
	for _, run := range log.Runs {
		rules += len(run.Tool.Driver.Rules)
		for _, result := range run.Results {
			locations += len(result.Locations)
			fingerprints += len(result.Fingerprints)
			partialFingerprints += len(result.PartialFingerprints)
			if result.RuleID != "" && len(result.Locations) == 0 && len(result.Fingerprints) == 0 && len(result.PartialFingerprints) == 0 {
				resultWithoutOptionalFields = true
			}
		}
	}
	if rules < 3 || locations < 2 || fingerprints == 0 || partialFingerprints < 2 || !resultWithoutOptionalFields {
		t.Fatalf("coverage missing: rules=%d locations=%d fingerprints=%d partialFingerprints=%d absentOptional=%t",
			rules, locations, fingerprints, partialFingerprints, resultWithoutOptionalFields)
	}
}

func TestInvalidFixtureBoundaries(t *testing.T) {
	malformed := readFixture(t, filepath.Join(sarifRoot, "invalid", "malformed-json.sarif.json"))
	if json.Valid(malformed) {
		t.Error("malformed fixture is valid JSON")
	}

	unsupported := readFixture(t, filepath.Join(sarifRoot, "invalid", "unsupported-version.sarif.json"))
	var log sarifLog
	if err := json.Unmarshal(unsupported, &log); err != nil {
		t.Fatalf("unsupported-version fixture should be valid JSON: %v", err)
	}
	if log.Version == sarifVersion {
		t.Errorf("unsupported version = %q, must differ from %q", log.Version, sarifVersion)
	}

	deeplyNested := readFixture(t, filepath.Join(sarifRoot, "invalid", "deeply-nested.sarif.json"))
	depth, err := maximumJSONDepth(deeplyNested)
	if err != nil {
		t.Fatalf("deeply-nested fixture should be valid JSON: %v", err)
	}
	if depth <= deepNestingBoundary {
		t.Errorf("maximum depth = %d, want greater than %d", depth, deepNestingBoundary)
	}

	oversized := readFixture(t, filepath.Join(sarifRoot, "invalid", "oversized.sarif.json"))
	if len(oversized) <= oversizedTestBoundary {
		t.Errorf("oversized fixture size = %d, want greater than %d", len(oversized), oversizedTestBoundary)
	}
	if !json.Valid(oversized) {
		t.Error("oversized fixture must remain valid JSON so only the byte limit is exercised")
	}
}

func TestSARIFCorpusSafetyAndDocumentation(t *testing.T) {
	readFixture(t, filepath.Join(sarifRoot, "README.md"))

	forbidden := []string{
		"-----BEGIN PRIVATE KEY-----",
		"ghp_",
		"github_pat_",
		"AKIA",
		"/home/",
		"/Users/",
		"@spectremi",
	}
	err := filepath.WalkDir(sarifRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if len(data) > fixtureSizeLimit {
			return fmt.Errorf("%s is %d bytes; limit is %d", path, len(data), fixtureSizeLimit)
		}
		for _, marker := range forbidden {
			if bytes.Contains(data, []byte(marker)) {
				return fmt.Errorf("%s contains forbidden marker %q", path, marker)
			}
		}
		if strings.Contains(string(data), "example.internal") {
			return fmt.Errorf("%s contains a non-reserved internal hostname", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func readFixture(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func maximumJSONDepth(data []byte) (int, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	depth, maximum := 0, 0
	for {
		token, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				return maximum, nil
			}
			return 0, err
		}
		switch token {
		case json.Delim('{'), json.Delim('['):
			depth++
			if depth > maximum {
				maximum = depth
			}
		case json.Delim('}'), json.Delim(']'):
			depth--
		}
	}
}
