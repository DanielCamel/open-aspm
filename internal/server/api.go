package server

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/spectremi/open-aspm/internal/authentication"
	"github.com/spectremi/open-aspm/internal/authorization"
	"github.com/spectremi/open-aspm/internal/catalog"
)

const maxJSONBodyBytes = 4 << 10

type authenticator interface {
	Authenticate(context.Context, string) (authentication.Subject, error)
}

type repositoryCreator interface {
	CreateRepository(context.Context, catalog.CreateRepositoryRequest) (catalog.RepositoryResult, error)
}

type contextKey string

const (
	requestIDContextKey contextKey = "request-id"
	subjectContextKey   contextKey = "authentication-subject"
)

type apiHandler struct {
	authenticator authenticator
	repositories  repositoryCreator
	logger        *slog.Logger
	requestID     func() string
}

type createRepositoryBody struct {
	DisplayName string `json:"display_name"`
}

type repositoryResponse struct {
	ID                   string    `json:"id"`
	WorkspaceID          string    `json:"workspace_id"`
	DisplayName          string    `json:"display_name"`
	CreatedByPrincipalID string    `json:"created_by_principal_id"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

type problemResponse struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	RequestID string `json:"request_id"`
}

// newAPIHandler returns the health surface plus the first authenticated API
// route. Authentication and authorization remain separate checks.
func newAPIHandler(
	authenticator authenticator,
	repositories repositoryCreator,
	logger *slog.Logger,
) (http.Handler, error) {
	if authenticator == nil || repositories == nil || logger == nil {
		return nil, errors.New("invalid HTTP API dependencies")
	}
	api := &apiHandler{
		authenticator: authenticator,
		repositories:  repositories,
		logger:        logger,
		requestID:     newRequestID,
	}
	return newHandler(api), nil
}

func (api *apiHandler) register(mux *http.ServeMux) {
	mux.Handle(
		"POST /api/v1/workspaces/{workspace_id}/repositories",
		api.authenticate(http.HandlerFunc(api.createRepository)),
	)
}

func (api *apiHandler) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requestID := api.requestID()
		response.Header().Set("X-Request-ID", requestID)
		ctx := context.WithValue(request.Context(), requestIDContextKey, requestID)

		plaintext, ok := bearerToken(request.Header.Values("Authorization"))
		if !ok {
			response.Header().Set("WWW-Authenticate", "Bearer")
			writeProblem(response, requestID, http.StatusUnauthorized, "unauthenticated", "Authentication required")
			return
		}
		subject, err := api.authenticator.Authenticate(ctx, plaintext)
		if errors.Is(err, authentication.ErrUnauthenticated) {
			response.Header().Set("WWW-Authenticate", "Bearer")
			writeProblem(response, requestID, http.StatusUnauthorized, "unauthenticated", "Authentication required")
			return
		}
		if err != nil {
			api.logger.Error("API authentication failed", "request_id", requestID, "error", err)
			writeProblem(response, requestID, http.StatusServiceUnavailable, "service-unavailable", "Service unavailable")
			return
		}
		if !validCallerRequestID(request.Header.Values("X-Request-ID")) {
			writeProblem(response, requestID, http.StatusBadRequest, "validation-failed", "Request validation failed")
			return
		}
		ctx = context.WithValue(ctx, subjectContextKey, subject)
		next.ServeHTTP(response, request.WithContext(ctx))
	})
}

func (api *apiHandler) createRepository(response http.ResponseWriter, request *http.Request) {
	requestID := requestIDFromContext(request.Context())
	subject, ok := subjectFromContext(request.Context())
	if !ok {
		writeProblem(response, requestID, http.StatusInternalServerError, "internal-error", "Internal server error")
		return
	}
	workspaceID := request.PathValue("workspace_id")
	if workspaceID != subject.WorkspaceID {
		writeProblem(response, requestID, http.StatusNotFound, "not-found", "Resource not found")
		return
	}
	idempotencyKey, ok := singleHeader(request.Header.Values("Idempotency-Key"))
	if !ok {
		writeProblem(response, requestID, http.StatusBadRequest, "validation-failed", "Request validation failed")
		return
	}
	contentType, ok := singleHeader(request.Header.Values("Content-Type"))
	if !ok {
		writeProblem(response, requestID, http.StatusBadRequest, "validation-failed", "Request validation failed")
		return
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "application/json" {
		writeProblem(response, requestID, http.StatusBadRequest, "validation-failed", "Request validation failed")
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, maxJSONBodyBytes)
	decoder := json.NewDecoder(request.Body)
	body, err := decodeCreateRepositoryBody(decoder)
	if err != nil {
		writeProblem(response, requestID, http.StatusBadRequest, "validation-failed", "Request validation failed")
		return
	}

	result, err := api.repositories.CreateRepository(request.Context(), catalog.CreateRepositoryRequest{
		PrincipalID: subject.PrincipalID, WorkspaceID: workspaceID,
		IdempotencyKey: idempotencyKey, DisplayName: body.DisplayName,
	})
	if err != nil {
		api.writeCatalogError(response, requestID, err)
		return
	}
	repository := result.Repository
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set(
		"Location",
		"/api/v1/workspaces/"+url.PathEscape(repository.WorkspaceID)+"/repositories/"+url.PathEscape(repository.ID),
	)
	response.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(response).Encode(repositoryResponse{
		ID: repository.ID, WorkspaceID: repository.WorkspaceID,
		DisplayName: repository.DisplayName, CreatedByPrincipalID: repository.CreatedByPrincipalID,
		CreatedAt: repository.CreatedAt, UpdatedAt: repository.UpdatedAt,
	})
}

func (api *apiHandler) writeCatalogError(response http.ResponseWriter, requestID string, err error) {
	switch {
	case errors.Is(err, catalog.ErrInvalid):
		writeProblem(response, requestID, http.StatusBadRequest, "validation-failed", "Request validation failed")
	case errors.Is(err, authorization.ErrDenied):
		writeProblem(response, requestID, http.StatusForbidden, "forbidden", "Operation forbidden")
	case errors.Is(err, catalog.ErrWorkspaceNotFound):
		writeProblem(response, requestID, http.StatusNotFound, "not-found", "Resource not found")
	case errors.Is(err, catalog.ErrIdempotencyConflict):
		writeProblem(response, requestID, http.StatusConflict, "idempotency-key-conflict", "Idempotency key conflict")
	case errors.Is(err, catalog.ErrIdempotencyInProgress):
		response.Header().Set("Retry-After", "1")
		writeProblem(response, requestID, http.StatusConflict, "idempotency-in-progress", "Request already in progress")
	default:
		api.logger.Error("repository creation failed", "request_id", requestID, "error", err)
		writeProblem(response, requestID, http.StatusInternalServerError, "internal-error", "Internal server error")
	}
}

func writeProblem(response http.ResponseWriter, requestID string, status int, code, title string) {
	response.Header().Set("Content-Type", "application/problem+json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(problemResponse{
		Type: "urn:open-aspm:problem:" + code, Title: title, Status: status,
		Code: code, RequestID: requestID,
	})
}

func bearerToken(values []string) (string, bool) {
	value, ok := singleHeader(values)
	if !ok {
		return "", false
	}
	parts := strings.Fields(value)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.Contains(parts[1], ",") {
		return "", false
	}
	return parts[1], true
}

func singleHeader(values []string) (string, bool) {
	if len(values) != 1 || values[0] == "" || strings.TrimSpace(values[0]) != values[0] {
		return "", false
	}
	return values[0], true
}

func validCallerRequestID(values []string) bool {
	if len(values) == 0 {
		return true
	}
	value, ok := singleHeader(values)
	if !ok || !utf8.ValidString(value) || utf8.RuneCountInString(value) > 128 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}
	return err
}

func decodeCreateRepositoryBody(decoder *json.Decoder) (createRepositoryBody, error) {
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return createRepositoryBody{}, errors.New("repository body must be an object")
	}
	var body createRepositoryBody
	displayNameSeen := false
	for decoder.More() {
		name, err := decoder.Token()
		if err != nil {
			return createRepositoryBody{}, err
		}
		if name != "display_name" || displayNameSeen {
			return createRepositoryBody{}, errors.New("unknown or duplicate repository field")
		}
		if err := decoder.Decode(&body.DisplayName); err != nil {
			return createRepositoryBody{}, err
		}
		displayNameSeen = true
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') || !displayNameSeen {
		return createRepositoryBody{}, errors.New("invalid repository body")
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return createRepositoryBody{}, err
	}
	return body, nil
}

func newRequestID() string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return "req_unavailable"
	}
	return "req_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(buffer))
}

func requestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDContextKey).(string)
	if requestID == "" {
		return "req_unavailable"
	}
	return requestID
}

func subjectFromContext(ctx context.Context) (authentication.Subject, bool) {
	subject, ok := ctx.Value(subjectContextKey).(authentication.Subject)
	return subject, ok
}
