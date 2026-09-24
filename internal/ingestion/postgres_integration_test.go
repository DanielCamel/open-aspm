//go:build integration

package ingestion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/spectremi/open-aspm/internal/blobstore"
	"github.com/spectremi/open-aspm/internal/blobstore/filesystem"
	"github.com/spectremi/open-aspm/internal/database"
	"github.com/spectremi/open-aspm/internal/jobqueue"
)

func TestPostgresReservationIdempotencyAndIsolation(t *testing.T) {
	service, db := openReservationService(t)
	ctx := context.Background()
	request := validRequest()
	size := int64(48127)
	request.ExpectedSize = &size
	request.OriginalFilename = "results.sarif"
	request.ExpectedSHA256 = "4f7f2e849fcb4517f07bca75f0cb56d042da07f86f9f86c34d828b8e25f0107a"

	created, err := service.Reserve(ctx, request)
	if err != nil || !created.Created {
		t.Fatalf("first Reserve() = (%+v, %v), want created", created, err)
	}
	replayed, err := service.Reserve(ctx, request)
	if err != nil || replayed.Created || replayed.Import.ID != created.Import.ID {
		t.Fatalf("replayed Reserve() = (%+v, %v)", replayed, err)
	}
	if replayed.Import.ExpectedSize == nil || *replayed.Import.ExpectedSize != size ||
		replayed.Import.ExpectedSHA256 != request.ExpectedSHA256 ||
		!replayed.Import.UploadExpiresAt.Equal(created.Import.UploadExpiresAt) {
		t.Fatalf("replayed import lost original response fields: %+v", replayed.Import)
	}
	service.config.MaxUploadBytes = 1
	policyReplay, err := service.Reserve(ctx, request)
	if err != nil || policyReplay.Created || policyReplay.Import.ID != created.Import.ID {
		t.Fatalf("replay after policy change = (%+v, %v)", policyReplay, err)
	}
	oversizedNew := request
	oversizedNew.IdempotencyKey = "new-under-lower-limit"
	if _, err := service.Reserve(ctx, oversizedNew); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("new oversized Reserve() error = %v, want ErrTooLarge", err)
	}
	service.config.MaxUploadBytes = 100 << 20

	conflict := request
	conflict.OriginalFilename = "different.sarif"
	if _, err := service.Reserve(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting Reserve() error = %v, want ErrIdempotencyConflict", err)
	}

	otherPrincipal := request
	otherPrincipal.PrincipalID = "principal-b"
	principalReservation, err := service.Reserve(ctx, otherPrincipal)
	if err != nil || !principalReservation.Created || principalReservation.Import.ID == created.Import.ID {
		t.Fatalf("other-principal Reserve() = (%+v, %v)", principalReservation, err)
	}
	otherWorkspace := request
	otherWorkspace.WorkspaceID = "workspace-b"
	otherWorkspace.ApplicationID = "application-b"
	workspaceReservation, err := service.Reserve(ctx, otherWorkspace)
	if err != nil || !workspaceReservation.Created || workspaceReservation.Import.ID == created.Import.ID {
		t.Fatalf("other-workspace Reserve() = (%+v, %v)", workspaceReservation, err)
	}

	wrongWorkspace := request
	wrongWorkspace.WorkspaceID = "workspace-b"
	wrongWorkspace.IdempotencyKey = "wrong-workspace"
	if _, err := service.Reserve(ctx, wrongWorkspace); !errors.Is(err, ErrApplicationNotFound) {
		t.Fatalf("cross-workspace Reserve() error = %v, want ErrApplicationNotFound", err)
	}

	var importCount, idempotencyCount, artifactCount int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM open_aspm.imports WHERE workspace_id = $1`, "workspace-a",
	).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM open_aspm.import_create_idempotency WHERE workspace_id = $1`, "workspace-a",
	).Scan(&idempotencyCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM open_aspm.raw_artifacts
		WHERE workspace_id = $1 AND state = 'pending'`, "workspace-a",
	).Scan(&artifactCount); err != nil {
		t.Fatal(err)
	}
	if importCount != 2 || idempotencyCount != 2 || artifactCount != 2 {
		t.Fatalf("stored counts = imports %d, idempotency %d, artifacts %d; want 2, 2, 2",
			importCount, idempotencyCount, artifactCount)
	}
	var retentionSeconds int64
	if err := db.QueryRowContext(ctx, `
		SELECT extract(epoch FROM (expires_at - completed_at))::bigint
		FROM open_aspm.import_create_idempotency
		WHERE workspace_id = $1 AND principal_id = $2 AND idempotency_key = $3`,
		request.WorkspaceID, request.PrincipalID, request.IdempotencyKey,
	).Scan(&retentionSeconds); err != nil {
		t.Fatal(err)
	}
	if retentionSeconds < int64((24*time.Hour)/time.Second) {
		t.Fatalf("idempotency retention = %ds, want at least 24h", retentionSeconds)
	}
}

func TestConcurrentReservationReplayCreatesOneImport(t *testing.T) {
	service, db := openReservationService(t)
	ctx := context.Background()
	lockedRequest := validRequest()
	lockedRequest.IdempotencyKey = "locked-request"
	lockSpec := reserveSpec{
		Import:      Import{WorkspaceID: lockedRequest.WorkspaceID},
		PrincipalID: lockedRequest.PrincipalID, APIMajorVersion: apiMajorVersion,
		Operation: operationCreateImport, IdempotencyKey: lockedRequest.IdempotencyKey,
	}
	lockTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"open-aspm-import-idempotency:"+idempotencyLockKey(lockSpec),
	); err != nil {
		_ = lockTx.Rollback()
		t.Fatal(err)
	}
	if _, err := service.Reserve(ctx, lockedRequest); !errors.Is(err, ErrIdempotencyInProgress) {
		_ = lockTx.Rollback()
		t.Fatalf("locked Reserve() error = %v, want ErrIdempotencyInProgress", err)
	}
	if err := lockTx.Rollback(); err != nil {
		t.Fatal(err)
	}

	request := validRequest()
	request.IdempotencyKey = "concurrent-request"
	const requestCount = 16
	type result struct {
		reservation Reservation
		err         error
	}
	results := make(chan result, requestCount)
	var wait sync.WaitGroup
	for index := 0; index < requestCount; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			reservation, err := service.Reserve(ctx, request)
			results <- result{reservation: reservation, err: err}
		}()
	}
	wait.Wait()
	close(results)

	createdCount := 0
	completedCount := 0
	var importID string
	inProgressCount := 0
	for result := range results {
		if errors.Is(result.err, ErrIdempotencyInProgress) {
			inProgressCount++
			continue
		}
		if result.err != nil {
			t.Fatalf("concurrent Reserve() error = %v", result.err)
		}
		completedCount++
		if importID == "" {
			importID = result.reservation.Import.ID
		} else if result.reservation.Import.ID != importID {
			t.Fatalf("concurrent import ID = %q, want %q", result.reservation.Import.ID, importID)
		}
		if result.reservation.Created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created results = %d, want one", createdCount)
	}
	if completedCount+inProgressCount != requestCount {
		t.Fatalf("accounted results = %d, want %d", completedCount+inProgressCount, requestCount)
	}
	finalReplay, err := service.Reserve(ctx, request)
	if err != nil || finalReplay.Created || finalReplay.Import.ID != importID {
		t.Fatalf("final replay = (%+v, %v)", finalReplay, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM open_aspm.imports
		WHERE workspace_id = $1 AND id = $2`, request.WorkspaceID, importID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("stored import count = %d, want one", count)
	}
}

func TestPostgresUploadLifecycleReplayAndIsolation(t *testing.T) {
	service, db := openReservationService(t)
	ctx := context.Background()
	content := []byte("synthetic integration evidence")
	digest := sha256.Sum256(content)
	size := int64(len(content))
	request := validRequest()
	request.ExpectedSize = &size
	request.ExpectedSHA256 = hex.EncodeToString(digest[:])
	reservation, err := service.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	upload := UploadRequest{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, ImportID: reservation.Import.ID,
		DeclaredSize: &size, Content: bytes.NewReader(content),
	}
	receipt, err := service.Upload(ctx, upload)
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if receipt.State != ImportUploaded || receipt.SizeBytes != size || receipt.SHA256 != request.ExpectedSHA256 {
		t.Fatalf("receipt = %+v", receipt)
	}

	var importState ImportState
	var artifactState rawArtifactState
	var storedSize int64
	var storedDigest []byte
	var storageKey, mediaType string
	if err := db.QueryRowContext(ctx, `
		SELECT imp.state, art.state, art.size_bytes, art.sha256,
		       art.storage_key, art.media_type_hint
		FROM open_aspm.imports AS imp
		JOIN open_aspm.raw_artifacts AS art
		  ON art.workspace_id = imp.workspace_id AND art.import_id = imp.id
		WHERE imp.workspace_id = $1 AND imp.id = $2`, request.WorkspaceID, reservation.Import.ID,
	).Scan(&importState, &artifactState, &storedSize, &storedDigest, &storageKey, &mediaType); err != nil {
		t.Fatal(err)
	}
	if importState != ImportUploaded || artifactState != rawArtifactCommitted ||
		storedSize != size || !bytes.Equal(storedDigest, digest[:]) || storageKey == "" ||
		mediaType != rawArtifactMediaType {
		t.Fatalf("stored upload = import %s artifact %s size %d digest %x key-present %t",
			importState, artifactState, storedSize, storedDigest, storageKey != "")
	}

	upload.Content = bytes.NewReader(content)
	replay, err := service.Upload(ctx, upload)
	if err != nil || replay.ImportID != receipt.ImportID || replay.State != receipt.State ||
		replay.SizeBytes != receipt.SizeBytes || replay.SHA256 != receipt.SHA256 ||
		!replay.UploadedAt.Equal(receipt.UploadedAt) {
		t.Fatalf("identical replay = (%+v, %v), want %+v", replay, err, receipt)
	}
	upload.Content = bytes.NewReader([]byte("conflicting integration evidence"))
	if _, err := service.Upload(ctx, upload); !errors.Is(err, ErrUploadConflict) {
		t.Fatalf("conflicting replay error = %v, want ErrUploadConflict", err)
	}

	wrongWorkspace := upload
	wrongWorkspace.WorkspaceID = "workspace-b"
	wrongWorkspace.ApplicationID = "application-b"
	wrongWorkspace.Content = bytes.NewReader(content)
	if _, err := service.Upload(ctx, wrongWorkspace); !errors.Is(err, ErrImportNotFound) {
		t.Fatalf("cross-workspace upload error = %v, want ErrImportNotFound", err)
	}
	wrongApplication := upload
	wrongApplication.ApplicationID = "application-other"
	wrongApplication.Content = bytes.NewReader(content)
	if _, err := service.Upload(ctx, wrongApplication); !errors.Is(err, ErrImportNotFound) {
		t.Fatalf("cross-application upload error = %v, want ErrImportNotFound", err)
	}
	wrongOwner := upload
	wrongOwner.PrincipalID = "principal-b"
	wrongOwner.Content = bytes.NewReader(content)
	if _, err := service.Upload(ctx, wrongOwner); !errors.Is(err, ErrImportNotFound) {
		t.Fatalf("cross-owner upload error = %v, want ErrImportNotFound", err)
	}
}

func TestPostgresCompletionCreatesOneOperationAndJob(t *testing.T) {
	service, db := openReservationService(t)
	ctx := context.Background()
	request := validRequest()
	reservation := reserveAndUpload(t, service, request, []byte("synthetic completion evidence"))
	complete := CompleteRequest{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, ImportID: reservation.Import.ID,
		IdempotencyKey: "complete-import",
	}

	created, err := service.Complete(ctx, complete)
	if err != nil || !created.Created {
		t.Fatalf("first Complete() = (%+v, %v), want created", created, err)
	}
	if created.Operation.WorkspaceID != request.WorkspaceID ||
		created.Operation.ImportID != reservation.Import.ID ||
		created.Operation.CreatedByPrincipalID != request.PrincipalID ||
		created.Operation.Kind != importProcessKind || created.Operation.State != OperationQueued {
		t.Fatalf("operation = %+v", created.Operation)
	}

	var importState ImportState
	var operationState OperationState
	var operationKind, operationID, principalID, capability, queue, jobKind string
	var jobState jobqueue.State
	var schemaVersion, maxAttempts int
	var payload []byte
	if err := db.QueryRowContext(ctx, `
		SELECT imp.state, op.state, op.kind,
		       job.operation_id, job.initiating_principal_id, job.system_capability,
		       job.queue, job.kind, job.schema_version, job.payload,
		       job.max_attempts, job.state
		FROM open_aspm.imports AS imp
		JOIN open_aspm.operations AS op
		  ON op.workspace_id = imp.workspace_id AND op.import_id = imp.id
		JOIN open_aspm.jobs AS job
		  ON job.workspace_id = op.workspace_id AND job.operation_id = op.id
		WHERE imp.workspace_id = $1 AND imp.id = $2`,
		request.WorkspaceID, reservation.Import.ID,
	).Scan(
		&importState, &operationState, &operationKind,
		&operationID, &principalID, &capability, &queue, &jobKind,
		&schemaVersion, &payload, &maxAttempts, &jobState,
	); err != nil {
		t.Fatal(err)
	}
	var jobPayload struct {
		ImportID string `json:"import_id"`
	}
	if err := json.Unmarshal(payload, &jobPayload); err != nil {
		t.Fatalf("decode job payload: %v", err)
	}
	if importState != ImportQueued || operationState != OperationQueued ||
		operationKind != importProcessKind || operationID != created.Operation.ID ||
		principalID != request.PrincipalID || capability != importProcessCapability ||
		queue != importProcessQueue || jobKind != importProcessKind ||
		schemaVersion != importProcessSchema || jobPayload.ImportID != reservation.Import.ID ||
		maxAttempts != 3 || jobState != jobqueue.StateQueued {
		t.Fatalf("stored completion = import %s operation %s/%s job %s/%s/%d payload %s attempts %d state %s",
			importState, operationKind, operationState, queue, jobKind, schemaVersion,
			payload, maxAttempts, jobState)
	}

	advancedAt := created.Operation.UpdatedAt.Add(time.Second)
	if _, err := db.ExecContext(ctx, `
		UPDATE open_aspm.operations
		SET state = 'running', started_at = $3, updated_at = $3
		WHERE workspace_id = $1 AND id = $2`,
		request.WorkspaceID, created.Operation.ID, advancedAt,
	); err != nil {
		t.Fatal(err)
	}
	replayed, err := service.Complete(ctx, complete)
	if err != nil || replayed.Created || replayed.Operation.ID != created.Operation.ID ||
		replayed.Operation.State != created.Operation.State ||
		!replayed.Operation.CreatedAt.Equal(created.Operation.CreatedAt) ||
		!replayed.Operation.UpdatedAt.Equal(created.Operation.UpdatedAt) {
		t.Fatalf("completion replay = (%+v, %v), want original %+v", replayed, err, created.Operation)
	}
	assertCompletionCounts(t, db, request.WorkspaceID, reservation.Import.ID, 1, 1, 1)
}

func TestPostgresCompletionConflictsAndIsolation(t *testing.T) {
	service, db := openReservationService(t)
	ctx := context.Background()
	request := validRequest()
	first := reserveAndUpload(t, service, request, []byte("first completion evidence"))
	complete := CompleteRequest{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, ImportID: first.Import.ID,
		IdempotencyKey: "complete-shared-key",
	}
	if _, err := service.Complete(ctx, complete); err != nil {
		t.Fatal(err)
	}

	secondRequest := request
	secondRequest.IdempotencyKey = "second-import"
	second := reserveAndUpload(t, service, secondRequest, []byte("second completion evidence"))
	conflictingKey := complete
	conflictingKey.ImportID = second.Import.ID
	if _, err := service.Complete(ctx, conflictingKey); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting idempotency key error = %v, want ErrIdempotencyConflict", err)
	}
	assertCompletionCounts(t, db, request.WorkspaceID, second.Import.ID, 0, 0, 0)

	differentKey := complete
	differentKey.IdempotencyKey = "different-completion-key"
	if _, err := service.Complete(ctx, differentKey); !errors.Is(err, ErrCompletionConflict) {
		t.Fatalf("repeat completion error = %v, want ErrCompletionConflict", err)
	}

	pendingRequest := request
	pendingRequest.IdempotencyKey = "pending-import"
	pending, err := service.Reserve(ctx, pendingRequest)
	if err != nil {
		t.Fatal(err)
	}
	premature := complete
	premature.ImportID = pending.Import.ID
	premature.IdempotencyKey = "premature-completion"
	if _, err := service.Complete(ctx, premature); !errors.Is(err, ErrCompletionConflict) {
		t.Fatalf("premature completion error = %v, want ErrCompletionConflict", err)
	}
	assertCompletionCounts(t, db, request.WorkspaceID, pending.Import.ID, 0, 0, 0)

	isolation := []struct {
		name   string
		mutate func(*CompleteRequest)
	}{
		{name: "workspace", mutate: func(value *CompleteRequest) {
			value.WorkspaceID = "workspace-b"
			value.ApplicationID = "application-b"
		}},
		{name: "application", mutate: func(value *CompleteRequest) {
			value.ApplicationID = "application-other"
		}},
		{name: "owner", mutate: func(value *CompleteRequest) {
			value.PrincipalID = "principal-b"
		}},
	}
	for _, test := range isolation {
		t.Run(test.name, func(t *testing.T) {
			value := complete
			value.IdempotencyKey = "isolated-" + test.name
			test.mutate(&value)
			if _, err := service.Complete(ctx, value); !errors.Is(err, ErrImportNotFound) {
				t.Fatalf("Complete() error = %v, want ErrImportNotFound", err)
			}
		})
	}
	assertCompletionCounts(t, db, request.WorkspaceID, first.Import.ID, 1, 1, 1)
}

func TestConcurrentCompletionCreatesOneOperationAndJob(t *testing.T) {
	service, db := openReservationService(t)
	ctx := context.Background()
	request := validRequest()
	reservation := reserveAndUpload(t, service, request, []byte("concurrent completion evidence"))
	complete := CompleteRequest{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, ImportID: reservation.Import.ID,
		IdempotencyKey: "concurrent-completion",
	}

	const requestCount = 16
	type result struct {
		completion Completion
		err        error
	}
	results := make(chan result, requestCount)
	var wait sync.WaitGroup
	for index := 0; index < requestCount; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			completion, err := service.Complete(ctx, complete)
			results <- result{completion: completion, err: err}
		}()
	}
	wait.Wait()
	close(results)

	createdCount := 0
	var operationID string
	for result := range results {
		if errors.Is(result.err, ErrIdempotencyInProgress) {
			continue
		}
		if result.err != nil {
			t.Fatalf("concurrent Complete() error = %v", result.err)
		}
		if operationID == "" {
			operationID = result.completion.Operation.ID
		} else if result.completion.Operation.ID != operationID {
			t.Fatalf("operation ID = %q, want %q", result.completion.Operation.ID, operationID)
		}
		if result.completion.Created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created completions = %d, want one", createdCount)
	}
	finalReplay, err := service.Complete(ctx, complete)
	if err != nil || finalReplay.Created || finalReplay.Operation.ID != operationID {
		t.Fatalf("final completion replay = (%+v, %v), want operation %q", finalReplay, err, operationID)
	}
	assertCompletionCounts(t, db, request.WorkspaceID, reservation.Import.ID, 1, 1, 1)
}

func TestCompletionRollsBackWhenQueueEnqueueFails(t *testing.T) {
	service, db := openReservationService(t)
	ctx := context.Background()
	request := validRequest()
	reservation := reserveAndUpload(t, service, request, []byte("rollback completion evidence"))
	store := service.store.(*PostgresStore)
	store.jobs = failingTransactionalEnqueuer{err: errors.New("synthetic enqueue failure")}
	complete := CompleteRequest{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, ImportID: reservation.Import.ID,
		IdempotencyKey: "failed-enqueue",
	}

	if _, err := service.Complete(ctx, complete); err == nil ||
		!strings.Contains(err.Error(), "enqueue import processing") {
		t.Fatalf("Complete() error = %v, want enqueue context", err)
	}
	var state ImportState
	if err := db.QueryRowContext(ctx, `
		SELECT state FROM open_aspm.imports WHERE workspace_id = $1 AND id = $2`,
		request.WorkspaceID, reservation.Import.ID,
	).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != ImportUploaded {
		t.Fatalf("import state = %s, want uploaded", state)
	}
	assertCompletionCounts(t, db, request.WorkspaceID, reservation.Import.ID, 0, 0, 0)
}

func TestPostgresUploadRecoversBlobAfterMetadataCommitFailure(t *testing.T) {
	service, _ := openReservationService(t)
	ctx := context.Background()
	content := []byte("evidence survives metadata failure")
	request := validRequest()
	reservation, err := service.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	baseStore := service.store.(*PostgresStore)
	failing := &failCommitOnceStore{PostgresStore: baseStore}
	service.store = failing
	upload := UploadRequest{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, ImportID: reservation.Import.ID,
		Content: bytes.NewReader(content),
	}
	if _, err := service.Upload(ctx, upload); err == nil || !failing.failed {
		t.Fatalf("first Upload() error = %v, failed-once = %t", err, failing.failed)
	}
	upload.Content = bytes.NewReader(content)
	receipt, err := service.Upload(ctx, upload)
	if err != nil {
		t.Fatalf("recovery Upload() error = %v", err)
	}
	if receipt.State != ImportUploaded || receipt.SizeBytes != int64(len(content)) {
		t.Fatalf("recovery receipt = %+v", receipt)
	}
}

func TestPostgresUploadLeaseFencesConcurrentAndStaleAttempts(t *testing.T) {
	service, _ := openReservationService(t)
	ctx := context.Background()
	request := validRequest()
	reservation, err := service.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	key, err := blobstore.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := service.store.(*PostgresStore)
	firstSpec := beginUploadSpec{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, ImportID: reservation.Import.ID,
		ArtifactID: "artifact-first", StorageBackend: "filesystem-test", StorageKey: key.String(),
		AttemptID: "attempt-first", StartedAt: now, LeaseExpiresAt: now.Add(time.Minute),
	}
	first, err := store.BeginUpload(ctx, firstSpec)
	if err != nil {
		t.Fatal(err)
	}
	secondSpec := firstSpec
	secondSpec.ArtifactID = "artifact-second"
	secondSpec.AttemptID = "attempt-second"
	secondSpec.StartedAt = now.Add(time.Second)
	secondSpec.LeaseExpiresAt = secondSpec.StartedAt.Add(time.Minute)
	if _, err := store.BeginUpload(ctx, secondSpec); !errors.Is(err, ErrUploadInProgress) {
		t.Fatalf("concurrent BeginUpload() error = %v, want ErrUploadInProgress", err)
	}

	secondSpec.StartedAt = firstSpec.LeaseExpiresAt.Add(time.Microsecond)
	secondSpec.LeaseExpiresAt = secondSpec.StartedAt.Add(time.Minute)
	second, err := store.BeginUpload(ctx, secondSpec)
	if err != nil {
		t.Fatal(err)
	}
	if second.ArtifactID != first.ArtifactID || second.StorageKey != first.StorageKey ||
		second.AttemptID != secondSpec.AttemptID {
		t.Fatalf("renewed session = %+v, first = %+v", second, first)
	}
	if _, err := store.CommitUpload(ctx, commitUploadSpec{
		WorkspaceID: request.WorkspaceID, ImportID: reservation.Import.ID,
		ArtifactID: first.ArtifactID, AttemptID: first.AttemptID,
		SizeBytes: 1, SHA256: strings.Repeat("0", 64), StorageVersion: "version-stale",
		CommittedAt: now.Add(30 * time.Second),
	}); !errors.Is(err, ErrUploadLeaseLost) {
		t.Fatalf("stale CommitUpload() error = %v, want ErrUploadLeaseLost", err)
	}
	if err := store.EndUpload(ctx, endUploadSpec{
		WorkspaceID: request.WorkspaceID, ImportID: reservation.Import.ID,
		ArtifactID: second.ArtifactID, AttemptID: second.AttemptID, EndedAt: secondSpec.StartedAt,
	}); err != nil {
		t.Fatal(err)
	}
}

type failCommitOnceStore struct {
	*PostgresStore
	failed bool
}

type failingTransactionalEnqueuer struct{ err error }

func (enqueuer failingTransactionalEnqueuer) EnqueueTx(
	context.Context,
	*sql.Tx,
	jobqueue.Spec,
) (jobqueue.Job, bool, error) {
	return jobqueue.Job{}, false, enqueuer.err
}

func (store *failCommitOnceStore) CommitUpload(ctx context.Context, spec commitUploadSpec) (UploadReceipt, error) {
	if !store.failed {
		store.failed = true
		return UploadReceipt{}, errors.New("synthetic metadata commit failure")
	}
	return store.PostgresStore.CommitUpload(ctx, spec)
}

type allowAuthorizer struct{}

func (allowAuthorizer) Authorize(context.Context, AuthorizationRequest) error { return nil }

func openReservationService(t *testing.T) (*Service, *sql.DB) {
	t.Helper()
	adminURL := os.Getenv("OPEN_ASPM_TEST_DATABASE_ADMIN_URL")
	if adminURL == "" {
		t.Skip("OPEN_ASPM_TEST_DATABASE_ADMIN_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	admin, err := sql.Open("pgx", adminURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("connect to test PostgreSQL: %v", err)
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	databaseName := "open_aspm_ingestion_" + suffix
	ownerRole := "open_aspm_ingestion_owner_" + suffix
	runtimeRole := "open_aspm_ingestion_runtime_" + suffix
	password := "integration-test-only"
	databaseIdentifier := pgx.Identifier{databaseName}.Sanitize()
	ownerIdentifier := pgx.Identifier{ownerRole}.Sanitize()
	runtimeIdentifier := pgx.Identifier{runtimeRole}.Sanitize()
	for _, role := range []string{ownerIdentifier, runtimeIdentifier} {
		if _, err := admin.ExecContext(ctx, fmt.Sprintf(
			"CREATE ROLE %s LOGIN PASSWORD 'integration-test-only' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION",
			role,
		)); err != nil {
			t.Fatalf("create test role: %v", err)
		}
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		"CREATE DATABASE %s OWNER %s", databaseIdentifier, ownerIdentifier,
	)); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", databaseIdentifier))
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf("DROP ROLE IF EXISTS %s", runtimeIdentifier))
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf("DROP ROLE IF EXISTS %s", ownerIdentifier))
	})

	ownerURL := ingestionTestDatabaseURL(t, adminURL, databaseName, ownerRole, password)
	migrator, err := database.Open(ctx, ownerURL)
	if err != nil {
		t.Fatalf("open migrator: %v", err)
	}
	if _, _, err := migrator.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := migrator.Close(); err != nil {
		t.Fatalf("close migrator: %v", err)
	}
	ownerDB, err := sql.Open("pgx", ownerURL)
	if err != nil {
		t.Fatal(err)
	}
	defer ownerDB.Close()
	for _, workspaceID := range []string{"workspace-a", "workspace-b"} {
		if _, err := ownerDB.ExecContext(ctx, `INSERT INTO open_aspm.workspaces (id) VALUES ($1)`, workspaceID); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
	}
	for _, application := range []struct{ workspaceID, id string }{
		{workspaceID: "workspace-a", id: "application-a"},
		{workspaceID: "workspace-a", id: "application-other"},
		{workspaceID: "workspace-b", id: "application-b"},
	} {
		if _, err := ownerDB.ExecContext(ctx, `
			INSERT INTO open_aspm.applications (workspace_id, id) VALUES ($1, $2)`,
			application.workspaceID, application.id,
		); err != nil {
			t.Fatalf("create application: %v", err)
		}
	}
	grants := []string{
		fmt.Sprintf("GRANT USAGE ON SCHEMA open_aspm TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT ON open_aspm.workspaces TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT ON open_aspm.applications TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE ON open_aspm.imports TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.import_create_idempotency TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE ON open_aspm.raw_artifacts TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE ON open_aspm.operations TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.import_complete_idempotency TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.jobs TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.scans TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.scan_scopes TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.observations TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.observation_locations TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.observation_fingerprints TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.observation_normalizations TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.import_parse_outputs TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.import_parse_warnings TO %s", runtimeIdentifier),
	}
	for _, grant := range grants {
		if _, err := ownerDB.ExecContext(ctx, grant); err != nil {
			t.Fatalf("grant ingestion runtime access: %v", err)
		}
	}

	runtimeURL := ingestionTestDatabaseURL(t, adminURL, databaseName, runtimeRole, password)
	db, err := sql.Open("pgx", runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(20)
	store, err := NewPostgresStore(db)
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := filesystem.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	service, err := NewService(store, blobs, allowAuthorizer{}, Config{
		MaxUploadBytes: 100 << 20, UploadReservationTTL: 30 * time.Minute,
		UploadTimeout: time.Minute, IdempotencyRetention: 24 * time.Hour,
		StorageBackend: "filesystem-test", ProcessMaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, db
}

func reserveAndUpload(t *testing.T, service *Service, request ReserveRequest, content []byte) Reservation {
	t.Helper()
	reservation, err := service.Reserve(context.Background(), request)
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if _, err := service.Upload(context.Background(), UploadRequest{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, ImportID: reservation.Import.ID,
		Content: bytes.NewReader(content),
	}); err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	return reservation
}

func assertCompletionCounts(
	t *testing.T,
	db *sql.DB,
	workspaceID, importID string,
	wantOperations, wantJobs, wantIdempotency int,
) {
	t.Helper()
	ctx := context.Background()
	var operations, jobs, idempotency int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM open_aspm.operations
		WHERE workspace_id = $1 AND import_id = $2`, workspaceID, importID,
	).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM open_aspm.jobs AS job
		JOIN open_aspm.operations AS op
		  ON op.workspace_id = job.workspace_id AND op.id = job.operation_id
		WHERE op.workspace_id = $1 AND op.import_id = $2`, workspaceID, importID,
	).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM open_aspm.import_complete_idempotency AS idem
		JOIN open_aspm.operations AS op
		  ON op.workspace_id = idem.workspace_id AND op.id = idem.operation_id
		WHERE op.workspace_id = $1 AND op.import_id = $2`, workspaceID, importID,
	).Scan(&idempotency); err != nil {
		t.Fatal(err)
	}
	if operations != wantOperations || jobs != wantJobs || idempotency != wantIdempotency {
		t.Fatalf("completion counts = operations %d, jobs %d, idempotency %d; want %d, %d, %d",
			operations, jobs, idempotency, wantOperations, wantJobs, wantIdempotency)
	}
}

func ingestionTestDatabaseURL(t *testing.T, baseURL, databaseName, user, password string) string {
	t.Helper()
	config, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	config.Path = "/" + databaseName
	config.User = url.UserPassword(user, password)
	return config.String()
}
