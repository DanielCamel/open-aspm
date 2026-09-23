//go:build integration

package ingestion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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
		StorageBackend: "filesystem-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, db
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
