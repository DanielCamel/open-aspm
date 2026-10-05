package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/spectremi/open-aspm/internal/authentication"
	"github.com/spectremi/open-aspm/internal/authorization"
	"github.com/spectremi/open-aspm/internal/catalog"
)

type capabilityAuthorizer interface {
	Authorize(context.Context, authorization.Request) error
}

type catalogAuthorizationAdapter struct {
	authorizer capabilityAuthorizer
}

// NewRuntimeHandler wires the PostgreSQL-backed authentication,
// authorization, and Catalog application boundaries into the HTTP surface.
func NewRuntimeHandler(
	db *sql.DB,
	authenticationConfig authentication.Config,
	logger *slog.Logger,
) (http.Handler, error) {
	if db == nil || logger == nil {
		return nil, errors.New("invalid HTTP runtime dependencies")
	}
	authenticationStore, err := authentication.NewPostgresStore(db)
	if err != nil {
		return nil, fmt.Errorf("configure HTTP authentication store: %w", err)
	}
	authenticator, err := authentication.NewService(authenticationStore, authenticationConfig)
	if err != nil {
		return nil, fmt.Errorf("configure HTTP authentication: %w", err)
	}
	authorizationStore, err := authorization.NewPostgresStore(db)
	if err != nil {
		return nil, fmt.Errorf("configure HTTP authorization store: %w", err)
	}
	authorizer, err := authorization.NewService(authorizationStore)
	if err != nil {
		return nil, fmt.Errorf("configure HTTP authorization: %w", err)
	}
	catalogStore, err := catalog.NewPostgresStore(db)
	if err != nil {
		return nil, fmt.Errorf("configure HTTP Catalog store: %w", err)
	}
	catalogService, err := catalog.NewService(
		catalogStore,
		catalogAuthorizationAdapter{authorizer: authorizer},
		catalog.Config{IdempotencyRetention: 24 * time.Hour},
	)
	if err != nil {
		return nil, fmt.Errorf("configure HTTP Catalog service: %w", err)
	}
	return newAPIHandler(authenticator, catalogService, logger)
}

func (adapter catalogAuthorizationAdapter) Authorize(
	ctx context.Context,
	request catalog.AuthorizationRequest,
) error {
	subject, ok := subjectFromContext(ctx)
	if !ok || subject.TokenID == "" || subject.PrincipalID != request.PrincipalID ||
		subject.WorkspaceID != request.WorkspaceID {
		return authorization.ErrDenied
	}
	return adapter.authorizer.Authorize(ctx, authorization.Request{
		PrincipalID: request.PrincipalID, WorkspaceID: request.WorkspaceID,
		ApplicationID: request.ApplicationID, Capability: request.Capability,
		TokenID: subject.TokenID,
	})
}
