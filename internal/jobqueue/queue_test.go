package jobqueue

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestFingerprintSpecIsCanonicalAndComplete(t *testing.T) {
	t.Parallel()
	base := Spec{
		WorkspaceID:           "wsp_test",
		OperationID:           "op_test",
		InitiatingPrincipalID: "svc_test",
		SystemCapability:      "imports:process",
		Queue:                 "ingestion",
		Kind:                  "parse-report",
		SchemaVersion:         1,
		Payload:               json.RawMessage(`{"import_id":"imp_test","sequence":9007199254740993}`),
		IdempotencyKey:        "same-key",
		Priority:              10,
		AvailableAt:           time.Date(2026, 9, 23, 10, 0, 0, 123456789, time.FixedZone("test", 3*60*60)),
		MaxAttempts:           3,
	}
	payloadA, fingerprintA, err := fingerprintSpec(base)
	if err != nil {
		t.Fatal(err)
	}
	reordered := base
	reordered.Payload = json.RawMessage(`{"sequence":9007199254740993,"import_id":"imp_test"}`)
	payloadB, fingerprintB, err := fingerprintSpec(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payloadA, payloadB) || !bytes.Equal(fingerprintA, fingerprintB) {
		t.Fatal("equivalent JSON produced different canonical input or fingerprint")
	}
	if !bytes.Contains(payloadA, []byte("9007199254740993")) {
		t.Fatalf("large JSON integer lost precision: %s", payloadA)
	}

	changed := base
	changed.SystemCapability = "imports:other"
	_, fingerprintChanged, err := fingerprintSpec(changed)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(fingerprintA, fingerprintChanged) {
		t.Fatal("authorization context was omitted from the request fingerprint")
	}
}

func TestValidateSpecRejectsUnsafeOrUnboundedInput(t *testing.T) {
	t.Parallel()
	valid := Spec{
		WorkspaceID:           "wsp_test",
		InitiatingPrincipalID: "svc_test",
		SystemCapability:      "imports:process",
		Queue:                 "ingestion",
		Kind:                  "parse-report",
		SchemaVersion:         1,
		Payload:               json.RawMessage(`{"import_id":"imp_test"}`),
		IdempotencyKey:        "same-key",
		MaxAttempts:           3,
	}
	tests := []struct {
		name   string
		mutate func(*Spec)
	}{
		{name: "missing workspace", mutate: func(spec *Spec) { spec.WorkspaceID = "" }},
		{name: "missing principal", mutate: func(spec *Spec) { spec.InitiatingPrincipalID = "" }},
		{name: "bad capability", mutate: func(spec *Spec) { spec.SystemCapability = "Imports Process" }},
		{name: "bad idempotency key", mutate: func(spec *Spec) { spec.IdempotencyKey = "contains whitespace" }},
		{name: "array payload", mutate: func(spec *Spec) { spec.Payload = json.RawMessage(`[]`) }},
		{name: "oversized payload", mutate: func(spec *Spec) {
			spec.Payload = json.RawMessage(`{"value":"` + strings.Repeat("a", maxPayloadBytes) + `"}`)
		}},
		{name: "priority", mutate: func(spec *Spec) { spec.Priority = 1001 }},
		{name: "attempts", mutate: func(spec *Spec) { spec.MaxAttempts = 101 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := valid
			test.mutate(&spec)
			if _, _, err := fingerprintSpec(spec); !errors.Is(err, ErrInvalid) {
				t.Fatalf("fingerprintSpec() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestLeaseIdentityValidation(t *testing.T) {
	t.Parallel()
	token, err := randomID("lease_", 32)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateLeaseIdentity("wsp_test", "job_test", token); err != nil {
		t.Fatalf("generated token rejected: %v", err)
	}
	if err := validateLeaseIdentity("wsp_test", "job_test", "lease_short"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("short token error = %v, want ErrInvalid", err)
	}
}

func TestLeaseDoesNotSerializeFencingToken(t *testing.T) {
	t.Parallel()
	lease := Lease{Job: Job{ID: "job_test"}, token: "lease_secret"}
	encoded, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(lease.FencingToken())) {
		t.Fatalf("serialized lease disclosed its fencing token: %s", encoded)
	}
	for _, formatted := range []string{fmt.Sprintf("%+v", lease), fmt.Sprintf("%#v", lease)} {
		if strings.Contains(formatted, lease.FencingToken()) {
			t.Fatalf("formatted lease disclosed its fencing token: %s", formatted)
		}
	}
}
