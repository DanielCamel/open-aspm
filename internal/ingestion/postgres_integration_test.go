//go:build integration

package ingestion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

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

	var importCount, idempotencyCount int
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
	if importCount != 2 || idempotencyCount != 2 {
		t.Fatalf("stored counts = imports %d, idempotency %d; want 2, 2", importCount, idempotencyCount)
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
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.imports TO %s", runtimeIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT ON open_aspm.import_create_idempotency TO %s", runtimeIdentifier),
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
	service, err := NewService(store, allowAuthorizer{}, Config{
		MaxUploadBytes: 100 << 20, UploadReservationTTL: 30 * time.Minute,
		IdempotencyRetention: 24 * time.Hour,
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
