SHELL := /bin/sh

OAPI_CODEGEN_VERSION := v2.8.0
SQLC_VERSION := v1.31.1
BUF_VERSION := v1.72.0
PROTOC_GEN_GO_VERSION := v1.36.11

.PHONY: generate generate-openapi generate-proto generate-sql lint test test-integration verify

generate: generate-openapi generate-proto generate-sql

generate-openapi:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION) --config api/openapi/oapi-codegen.yaml api/openapi/vela.yaml

generate-proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) lint
	go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) generate

generate-sql:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION) generate

lint:
	go vet ./...
	golangci-lint run ./...

test:
	go test ./...

test-integration:
	go test -tags=integration ./internal/integration/...

verify: generate
	git diff --exit-code -- api/gen internal/store/sqlc proto/gen
	$(MAKE) lint
	$(MAKE) test
