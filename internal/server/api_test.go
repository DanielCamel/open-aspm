package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/spectremi/open-aspm/internal/authentication"
	"github.com/spectremi/open-aspm/internal/authorization"
	"github.com/spectremi/open-aspm/internal/catalog"
)

type authenticatorStub struct {
	subject authentication.Subject
	err     error
	tokens  []string
}

func (stub *authenticatorStub) Authenticate(_ context.Context, token string) (authentication.Subject, error) {
	stub.tokens = append(stub.tokens, token)
	return stub.subject, stub.err
}

type repositoryCreatorStub struct {
	requests []catalog.CreateRepositoryRequest
	result   catalog.RepositoryResult
	err      error
}

func (stub *repositoryCreatorStub) CreateRepository(
	_ context.Context,
	request catalog.CreateRepositoryRequest,
) (catalog.RepositoryResult, error) {
	stub.requests = append(stub.requests, request)
	return stub.result, stub.err
}

type capabilityAuthorizerStub struct {
	requests []authorization.Request
	err      error
}

func (stub *capabilityAuthorizerStub) Authorize(_ context.Context, request authorization.Request) error {
	stub.requests = append(stub.requests, request)
	return stub.err
}

func TestCreateRepositoryHTTP(t *testing.T) {
	fixedTime := time.Date(2026, time.October, 5, 10, 0, 0, 123000000, time.UTC)
	authenticator := &authenticatorStub{subject: authentication.Subject{
		TokenID: "token-a", PrincipalID: "principal-a", WorkspaceID: "workspace-a",
	}}
	repositories := &repositoryCreatorStub{result: catalog.RepositoryResult{
		Repository: catalog.Repository{
			ID: "repo_example", WorkspaceID: "workspace-a", DisplayName: "example-service",
			CreatedByPrincipalID: "principal-a", CreatedAt: fixedTime, UpdatedAt: fixedTime,
		},
		Created: true,
	}}
	handler, _ := newTestAPIHandler(authenticator, repositories, io.Discard)
	request := authenticatedRepositoryRequest(`{"display_name":"example-service"}`)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(authenticator.tokens) != 1 || authenticator.tokens[0] != "test-token" {
		t.Fatalf("authenticated tokens = %v", authenticator.tokens)
	}
	wantRequest := catalog.CreateRepositoryRequest{
		PrincipalID: "principal-a", WorkspaceID: "workspace-a",
		IdempotencyKey: "request-1", DisplayName: "example-service",
	}
	if len(repositories.requests) != 1 || repositories.requests[0] != wantRequest {
		t.Fatalf("repository requests = %+v", repositories.requests)
	}
	if got := response.Header().Get("Location"); got != "/api/v1/workspaces/workspace-a/repositories/repo_example" {
		t.Fatalf("Location = %q", got)
	}
	if response.Header().Get("X-Request-ID") != "req_test" ||
		response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("response headers = %+v", response.Header())
	}
	var body repositoryResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.ID != "repo_example" || body.DisplayName != "example-service" ||
		!body.CreatedAt.Equal(fixedTime) {
		t.Fatalf("response body = %+v", body)
	}
}

func TestCreateRepositoryReplayStillReturnsCreated(t *testing.T) {
	authenticator := validAuthenticator()
	repositories := &repositoryCreatorStub{result: catalog.RepositoryResult{
		Repository: catalog.Repository{
			ID: "repo_replayed", WorkspaceID: "workspace-a", DisplayName: "existing",
			CreatedByPrincipalID: "principal-a", CreatedAt: time.Now(), UpdatedAt: time.Now(),
		},
		Created: false,
	}}
	handler, _ := newTestAPIHandler(authenticator, repositories, io.Discard)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedRepositoryRequest(`{"display_name":"existing"}`))
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", response.Code)
	}
}

func TestCreateRepositoryRequiresAuthenticationBeforeParsingBody(t *testing.T) {
	authenticator := validAuthenticator()
	repositories := &repositoryCreatorStub{}
	handler, _ := newTestAPIHandler(authenticator, repositories, io.Discard)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/workspaces/workspace-a/repositories",
		strings.NewReader(`{"display_name":`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	assertProblem(t, response, http.StatusUnauthorized, "unauthenticated")
	if len(authenticator.tokens) != 0 || len(repositories.requests) != 0 {
		t.Fatal("unauthenticated request reached an application dependency")
	}
	if response.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("WWW-Authenticate = %q", response.Header().Get("WWW-Authenticate"))
	}
}

func TestCreateRepositoryConcealsOtherWorkspace(t *testing.T) {
	authenticator := validAuthenticator()
	repositories := &repositoryCreatorStub{}
	handler, _ := newTestAPIHandler(authenticator, repositories, io.Discard)
	request := authenticatedRepositoryRequest(`{"display_name":"example"}`)
	request.URL.Path = "/api/v1/workspaces/workspace-b/repositories"
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	assertProblem(t, response, http.StatusNotFound, "not-found")
	if len(repositories.requests) != 0 {
		t.Fatal("cross-workspace request reached Catalog service")
	}
}

func TestCreateRepositoryRejectsAmbiguousOrInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*http.Request)
		body   string
	}{
		{name: "unknown JSON field", body: `{"display_name":"example","unknown":true}`},
		{name: "duplicate JSON field", body: `{"display_name":"first","display_name":"second"}`},
		{name: "second JSON value", body: `{"display_name":"example"} {}`},
		{name: "missing content type", body: `{"display_name":"example"}`, mutate: func(request *http.Request) {
			request.Header.Del("Content-Type")
		}},
		{name: "duplicate idempotency key", body: `{"display_name":"example"}`, mutate: func(request *http.Request) {
			request.Header.Add("Idempotency-Key", "request-2")
		}},
		{name: "duplicate content type", body: `{"display_name":"example"}`, mutate: func(request *http.Request) {
			request.Header.Add("Content-Type", "application/json")
		}},
		{name: "duplicate authorization", body: `{"display_name":"example"}`, mutate: func(request *http.Request) {
			request.Header.Add("Authorization", "Bearer another-token")
		}},
		{name: "duplicate request ID", body: `{"display_name":"example"}`, mutate: func(request *http.Request) {
			request.Header.Add("X-Request-ID", "caller-1")
			request.Header.Add("X-Request-ID", "caller-2")
		}},
		{name: "oversized request ID", body: `{"display_name":"example"}`, mutate: func(request *http.Request) {
			request.Header.Set("X-Request-ID", strings.Repeat("a", 129))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator := validAuthenticator()
			repositories := &repositoryCreatorStub{}
			handler, _ := newTestAPIHandler(authenticator, repositories, io.Discard)
			request := authenticatedRepositoryRequest(test.body)
			if test.mutate != nil {
				test.mutate(request)
			}
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			wantStatus, wantCode := http.StatusBadRequest, "validation-failed"
			if test.name == "duplicate authorization" {
				wantStatus, wantCode = http.StatusUnauthorized, "unauthenticated"
			}
			assertProblem(t, response, wantStatus, wantCode)
			if len(repositories.requests) != 0 {
				t.Fatal("invalid request reached Catalog service")
			}
		})
	}
}

func TestCreateRepositoryMapsApplicationErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "invalid", err: catalog.ErrInvalid, wantStatus: 400, wantCode: "validation-failed"},
		{name: "denied", err: authorization.ErrDenied, wantStatus: 403, wantCode: "forbidden"},
		{name: "workspace", err: catalog.ErrWorkspaceNotFound, wantStatus: 404, wantCode: "not-found"},
		{name: "conflict", err: catalog.ErrIdempotencyConflict, wantStatus: 409, wantCode: "idempotency-key-conflict"},
		{name: "in progress", err: catalog.ErrIdempotencyInProgress, wantStatus: 409, wantCode: "idempotency-in-progress"},
		{name: "internal", err: errors.New("database unavailable"), wantStatus: 500, wantCode: "internal-error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repositories := &repositoryCreatorStub{err: test.err}
			handler, _ := newTestAPIHandler(validAuthenticator(), repositories, io.Discard)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, authenticatedRepositoryRequest(`{"display_name":"example"}`))
			assertProblem(t, response, test.wantStatus, test.wantCode)
			if test.err == catalog.ErrIdempotencyInProgress && response.Header().Get("Retry-After") != "1" {
				t.Fatalf("Retry-After = %q", response.Header().Get("Retry-After"))
			}
		})
	}
}

func TestAuthenticationBackendFailureDoesNotLogToken(t *testing.T) {
	var logs bytes.Buffer
	authenticator := validAuthenticator()
	authenticator.err = errors.New("authentication database unavailable")
	handler, _ := newTestAPIHandler(authenticator, &repositoryCreatorStub{}, &logs)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedRepositoryRequest(`{"display_name":"example"}`))
	assertProblem(t, response, http.StatusServiceUnavailable, "service-unavailable")
	if strings.Contains(logs.String(), "test-token") {
		t.Fatal("authentication failure log exposed bearer token")
	}
}

func TestCatalogAuthorizationAdapterRequiresMatchingTokenSubject(t *testing.T) {
	backend := &capabilityAuthorizerStub{}
	adapter := catalogAuthorizationAdapter{authorizer: backend}
	request := catalog.AuthorizationRequest{
		PrincipalID: "principal-a", WorkspaceID: "workspace-a",
		Capability: catalog.CapabilityRepositoriesCreate,
	}
	ctx := context.WithValue(context.Background(), subjectContextKey, authentication.Subject{
		TokenID: "token-a", PrincipalID: "principal-a", WorkspaceID: "workspace-a",
	})
	if err := adapter.Authorize(ctx, request); err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}
	want := authorization.Request{
		PrincipalID: "principal-a", WorkspaceID: "workspace-a",
		Capability: catalog.CapabilityRepositoriesCreate, TokenID: "token-a",
	}
	if len(backend.requests) != 1 || backend.requests[0] != want {
		t.Fatalf("authorization requests = %+v", backend.requests)
	}

	mismatched := context.WithValue(context.Background(), subjectContextKey, authentication.Subject{
		TokenID: "token-a", PrincipalID: "principal-b", WorkspaceID: "workspace-a",
	})
	if err := adapter.Authorize(mismatched, request); !errors.Is(err, authorization.ErrDenied) {
		t.Fatalf("mismatched Authorize() error = %v", err)
	}
}

func newTestAPIHandler(
	authenticator authenticator,
	repositories repositoryCreator,
	logOutput io.Writer,
) (http.Handler, *apiHandler) {
	api := &apiHandler{
		authenticator: authenticator,
		repositories:  repositories,
		logger:        slog.New(slog.NewJSONHandler(logOutput, nil)),
		requestID:     func() string { return "req_test" },
	}
	return newHandler(api), api
}

func validAuthenticator() *authenticatorStub {
	return &authenticatorStub{subject: authentication.Subject{
		TokenID: "token-a", PrincipalID: "principal-a", WorkspaceID: "workspace-a",
	}}
}

func authenticatedRepositoryRequest(body string) *http.Request {
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/workspaces/workspace-a/repositories",
		strings.NewReader(body),
	)
	request.Header.Set("Authorization", "Bearer test-token")
	request.Header.Set("Idempotency-Key", "request-1")
	request.Header.Set("Content-Type", "application/json")
	return request
}

func assertProblem(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, status, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("Content-Type = %q", response.Header().Get("Content-Type"))
	}
	var problem problemResponse
	if err := json.NewDecoder(response.Body).Decode(&problem); err != nil {
		t.Fatal(err)
	}
	if problem.Code != code || problem.Status != status || problem.RequestID != "req_test" {
		t.Fatalf("problem = %+v", problem)
	}
}
