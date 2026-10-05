# Catalog API v1 contract

The normative machine-readable contract is
[`openapi.yaml`](openapi.yaml). Repository creation is available through the
authenticated HTTP server. Application-to-Repository linking remains an
implemented internal service that is not wired into the server yet.

## Purpose and workflow

The first Catalog API exposes only the minimum vendor-neutral identity needed
to attribute an import safely:

```text
POST Repository                         -> 201 Repository
PUT Application-to-Repository link      -> 201 new | 200 existing
POST Import with analysis_context       -> 201 Import
```

Repository creation and linking are separate operations. A failure during the
link step leaves an ordinary unlinked Repository; clients may retry either
operation independently. The future CLI may wrap both calls without changing
server ownership or authorization.

## Repository identity

`POST /workspaces/{workspace_id}/repositories` requires
`catalog:repositories:create` and an `Idempotency-Key`. The body contains only
`display_name`. The server assigns an opaque Repository ID.

Display names are mutable, non-unique presentation metadata. Two requests with
different idempotency keys create distinct Repositories even when their names
match. A name, URL, path, provider object ID, or report field is never accepted
as proof that two Repository identities are the same.

Repository creation uses the same idempotency scope as import reservation:

```text
authenticated principal + workspace + API major version + operation + key
```

The completed result is retained for at least 24 hours. An identical replay
returns the original `201` response; changed validated input returns
`409 idempotency-key-conflict`.

## Application relationship

`PUT /workspaces/{workspace_id}/applications/{application_id}/repositories/{repository_id}`
requires `catalog:application-repositories:link` for the Application. It
returns `201` when it creates a new active relationship and `200` when that
active relationship already exists. Concurrent identical requests converge on
one relationship through a durable uniqueness constraint.

The relationship has its own opaque ID and a temporal interval. One Repository
may be linked explicitly to more than one Application. A future operation that
ends a relationship will set `valid_until`; it will not delete history. That
end operation is not part of this contract.

## Authorization and concealment

Catalog mutation capabilities are not implied by ingestion capabilities and
must be granted separately. Repository creation is workspace-scoped. Linking
also intersects the principal, token, workspace, Application scope, and the
Repository's workspace.

Unknown, cross-workspace, and out-of-scope Applications or Repositories are
concealed as `404`. Possessing an opaque identifier does not grant access.
Errors must not include Repository names, internal storage data, credentials,
or information about a resource outside the caller's effective scope.

## Deliberately deferred

- Repository list and get operations;
- rename and relationship-end operations;
- provider identities, URLs, aliases, and connector-managed discovery;
- inline Catalog creation during import reservation; and
- automatic matching by display name or report content.
