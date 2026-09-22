package api_test

import (
	"context"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestIngestionContractIsValidOpenAPI(t *testing.T) {
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}
	if err := document.Validate(context.Background()); err != nil {
		t.Fatalf("validate OpenAPI document: %v", err)
	}
}

func TestEveryOperationDeclaresCapabilities(t *testing.T) {
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}

	for path, item := range document.Paths.Map() {
		for method, operation := range item.Operations() {
			value, ok := operation.Extensions["x-required-capabilities"]
			if !ok {
				t.Errorf("%s %s has no x-required-capabilities", method, path)
				continue
			}
			capabilities, ok := value.([]any)
			if !ok || len(capabilities) == 0 {
				t.Errorf("%s %s has invalid x-required-capabilities: %#v", method, path, value)
			}
		}
	}
}

func TestIngestionWorkflowContract(t *testing.T) {
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}

	imports := document.Paths.Find("/workspaces/{workspace_id}/imports")
	if imports == nil || imports.Post == nil {
		t.Fatal("create import operation is missing")
	}
	assertRequiredHeader(t, imports.Post, "Idempotency-Key")
	if imports.Post.Responses.Value("201") == nil {
		t.Error("create import must return 201")
	}

	content := document.Paths.Find("/workspaces/{workspace_id}/imports/{import_id}/content")
	if content == nil || content.Put == nil || content.Put.RequestBody == nil {
		t.Fatal("streaming upload operation is missing")
	}
	if content.Put.RequestBody.Value.Content.Get("application/octet-stream") == nil {
		t.Error("upload must accept application/octet-stream")
	}
	if content.Put.Responses.Value("413") == nil {
		t.Error("upload must document size-limit failure")
	}

	complete := document.Paths.Find("/workspaces/{workspace_id}/imports/{import_id}/complete")
	if complete == nil || complete.Post == nil {
		t.Fatal("complete import operation is missing")
	}
	assertRequiredHeader(t, complete.Post, "Idempotency-Key")
	accepted := complete.Post.Responses.Value("202")
	if accepted == nil || accepted.Value == nil {
		t.Fatal("complete import must return 202")
	}
	if accepted.Value.Headers["Location"] == nil {
		t.Error("202 response must provide Location")
	}

	operation := document.Paths.Find("/workspaces/{workspace_id}/operations/{operation_id}")
	if operation == nil || operation.Get == nil {
		t.Fatal("operation status endpoint is missing")
	}
}

func TestErrorsUseProblemDetails(t *testing.T) {
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}

	for path, item := range document.Paths.Map() {
		for method, operation := range item.Operations() {
			for status, response := range operation.Responses.Map() {
				if status[0] != '4' && status[0] != '5' {
					continue
				}
				if response.Value.Content.Get("application/problem+json") == nil {
					t.Errorf("%s %s response %s is not application/problem+json", method, path, status)
				}
			}
		}
	}
}

func assertRequiredHeader(t *testing.T, operation *openapi3.Operation, name string) {
	t.Helper()
	parameter := operation.Parameters.GetByInAndName("header", name)
	if parameter == nil || !parameter.Required {
		t.Errorf("%s must be a required header", name)
	}
}
