SHELL := /bin/sh

OAPI_CODEGEN_VERSION := v2.8.0
SQLC_VERSION := v1.31.1
BUF_VERSION := v1.72.0
PROTOC_GEN_GO_VERSION := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2
GOLANGCI_LINT_VERSION := v2.13.1
INTEGRATION_TEST_TIMEOUT ?= 40m
INTEGRATION_TEST_SHARD_INDEX ?=
INTEGRATION_TEST_SHARD_TOTAL ?=
LAUNCH_RECEIPTS ?=
RELEASE_BUNDLE_PLAN ?=
RELEASE_BUNDLE ?=
RELEASE_REVISION ?=
RELEASE_ARTIFACT_DIR ?=
RELEASE_IMAGE_PREFIX ?=
H3_MOCK_BACKEND_CONTEXT ?=
H3_DISPOSABLE_IMAGE ?= vela-h3-member-campaign:disposable
H3_RUNTIME_BASE ?=
H3_RUNTIME_COMMAND_CONTEXT ?=
H3_ENCODER_SHA256 ?=
H3_DIT_SHA256 ?=
H3_VAE_DECODER_SHA256 ?=
H3_EVIDENCE_PLAN_REVISION ?=
H3_CAMPAIGN_MANIFEST ?=
TOOLS_BIN := $(CURDIR)/bin
VELA_IMAGE_BUILD_ARGUMENTS = "$(CURDIR)" "$(RELEASE_REVISION)" "$(RELEASE_IMAGE_PREFIX)" \
	"$(H3_RUNTIME_BASE)" "$(H3_RUNTIME_COMMAND_CONTEXT)" \
	"$(H3_ENCODER_SHA256)" "$(H3_DIT_SHA256)" "$(H3_VAE_DECODER_SHA256)"

.PHONY: generate generate-openapi generate-proto generate-sql verify-generated build-h3-mock-backend build-h3-disposable-member-campaign-image test-h3-disposable-member-campaign build-host-packages print-vela-image-build build-vela-images build-vela-image-artifacts publish-vela-images build-release-bundle verify-release-bundle preflight-h3-real-environment capture-h3-launch-evidence run-h3-campaign capture-h3-campaign-evidence build-h3-fault-campaign-evidence verify-launch lint test test-integration test-integration-shard test-cnpg-failover test-cnpg-pitr test-cross validate-deployment verify

generate: generate-openapi generate-proto generate-sql

generate-openapi:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION) --config api/openapi/oapi-codegen.yaml api/openapi/vela.yaml

generate-proto:
	mkdir -p $(TOOLS_BIN)
	GOBIN=$(TOOLS_BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(TOOLS_BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) lint
	go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) generate

generate-sql:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION) generate

lint:
	go vet ./...
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

test:
	go test ./...

test-integration:
	go test -tags=integration ./internal/integration/... -count=1 -timeout=$(INTEGRATION_TEST_TIMEOUT)

test-integration-shard:
	@test -n "$(INTEGRATION_TEST_SHARD_INDEX)" || \
		(echo "INTEGRATION_TEST_SHARD_INDEX is required" >&2; exit 2)
	@test -n "$(INTEGRATION_TEST_SHARD_TOTAL)" || \
		(echo "INTEGRATION_TEST_SHARD_TOTAL is required" >&2; exit 2)
	INTEGRATION_TEST_TIMEOUT="$(INTEGRATION_TEST_TIMEOUT)" \
		sh ./hack/test-integration-shard.sh \
		"$(INTEGRATION_TEST_SHARD_INDEX)" "$(INTEGRATION_TEST_SHARD_TOTAL)"

build-h3-mock-backend:
	@test -n "$(H3_MOCK_BACKEND_CONTEXT)" || \
		(echo "H3_MOCK_BACKEND_CONTEXT is required" >&2; exit 2)
	go run ./cmd/vela-release-artifacts build-h3-mock-backend \
		"$(CURDIR)" "$(H3_MOCK_BACKEND_CONTEXT)"

build-h3-disposable-member-campaign-image:
	docker build \
		--build-arg RELEASE_REVISION="$$(git rev-parse HEAD)" \
		--file deploy/h3-disposable-campaign/Dockerfile \
		--tag "$(H3_DISPOSABLE_IMAGE)" \
		.

test-h3-disposable-member-campaign:
	H3_DISPOSABLE_IMAGE="$(H3_DISPOSABLE_IMAGE)" ./hack/run-h3-disposable-member-campaign.sh

build-host-packages:
	@test -n "$(RELEASE_REVISION)" || \
		(echo "RELEASE_REVISION is required" >&2; exit 2)
	@test -n "$(RELEASE_ARTIFACT_DIR)" || \
		(echo "RELEASE_ARTIFACT_DIR is required" >&2; exit 2)
	go run ./cmd/vela-release-artifacts build-host-packages \
		"$(CURDIR)" "$(RELEASE_REVISION)" "$(RELEASE_ARTIFACT_DIR)"

print-vela-image-build:
	@go run ./cmd/vela-release-artifacts print-vela-image-build \
		$(VELA_IMAGE_BUILD_ARGUMENTS)

build-vela-images:
	go run ./cmd/vela-release-artifacts build-vela-images \
		$(VELA_IMAGE_BUILD_ARGUMENTS)

build-vela-image-artifacts:
	@test -n "$(RELEASE_ARTIFACT_DIR)" || \
		(echo "RELEASE_ARTIFACT_DIR is required" >&2; exit 2)
	@go run ./cmd/vela-release-artifacts build-vela-image-artifacts \
		$(VELA_IMAGE_BUILD_ARGUMENTS) "$(RELEASE_ARTIFACT_DIR)"

publish-vela-images:
	@test -n "$(RELEASE_ARTIFACT_DIR)" || \
		(echo "RELEASE_ARTIFACT_DIR is required" >&2; exit 2)
	@go run ./cmd/vela-release-artifacts publish-vela-images \
		$(VELA_IMAGE_BUILD_ARGUMENTS) "$(RELEASE_ARTIFACT_DIR)"

build-release-bundle:
	@test -n "$(RELEASE_BUNDLE_PLAN)" || \
		(echo "RELEASE_BUNDLE_PLAN is required" >&2; exit 2)
	@test -n "$(RELEASE_BUNDLE)" || \
		(echo "RELEASE_BUNDLE is required" >&2; exit 2)
	go run ./cmd/vela-release-bundle build "$(RELEASE_BUNDLE_PLAN)" "$(RELEASE_BUNDLE)"

verify-release-bundle:
	@test -n "$(RELEASE_BUNDLE)" || \
		(echo "RELEASE_BUNDLE is required" >&2; exit 2)
	go run ./cmd/vela-release-bundle verify "$(RELEASE_BUNDLE)"

preflight-h3-real-environment:
	@test -n "$(RELEASE_BUNDLE)" || \
		(echo "RELEASE_BUNDLE is required" >&2; exit 2)
	@test -n "$(H3_EVIDENCE_PLAN_REVISION)" || \
		(echo "H3_EVIDENCE_PLAN_REVISION is required" >&2; exit 2)
	go run ./cmd/vela-h3-evidence preflight \
		"$(RELEASE_BUNDLE)" "$(H3_EVIDENCE_PLAN_REVISION)"

capture-h3-launch-evidence:
	@test -n "$(RELEASE_BUNDLE)" || \
		(echo "RELEASE_BUNDLE is required" >&2; exit 2)
	@test -n "$(H3_EVIDENCE_PLAN_REVISION)" || \
		(echo "H3_EVIDENCE_PLAN_REVISION is required" >&2; exit 2)
	go run ./cmd/vela-h3-evidence capture \
		"$(RELEASE_BUNDLE)" "$(H3_EVIDENCE_PLAN_REVISION)"

run-h3-campaign:
	@test -n "$(RELEASE_BUNDLE)" || \
		(echo "RELEASE_BUNDLE is required" >&2; exit 2)
	@test -n "$(H3_EVIDENCE_PLAN_REVISION)" || \
		(echo "H3_EVIDENCE_PLAN_REVISION is required" >&2; exit 2)
	@test -n "$(H3_CAMPAIGN_MANIFEST)" || \
		(echo "H3_CAMPAIGN_MANIFEST is required" >&2; exit 2)
	go run ./cmd/vela-h3-evidence run-campaign \
		"$(RELEASE_BUNDLE)" "$(H3_EVIDENCE_PLAN_REVISION)" \
		"$(H3_CAMPAIGN_MANIFEST)"

capture-h3-campaign-evidence:
	@test -n "$(RELEASE_BUNDLE)" || \
		(echo "RELEASE_BUNDLE is required" >&2; exit 2)
	@test -n "$(H3_EVIDENCE_PLAN_REVISION)" || \
		(echo "H3_EVIDENCE_PLAN_REVISION is required" >&2; exit 2)
	@test -n "$(H3_SAME_NODE_JOB_ID)" || \
		(echo "H3_SAME_NODE_JOB_ID is required" >&2; exit 2)
	@test -n "$(H3_CROSS_NODE_JOB_ID)" || \
		(echo "H3_CROSS_NODE_JOB_ID is required" >&2; exit 2)
	@test -n "$(H3_CACHE_JOB_ID)" || \
		(echo "H3_CACHE_JOB_ID is required" >&2; exit 2)
	go run ./cmd/vela-h3-evidence capture-campaign \
		"$(RELEASE_BUNDLE)" "$(H3_EVIDENCE_PLAN_REVISION)" \
		"$(H3_SAME_NODE_JOB_ID)" "$(H3_CROSS_NODE_JOB_ID)" "$(H3_CACHE_JOB_ID)"

build-h3-fault-campaign-evidence:
	@test -n "$(H3_FAULT_CAMPAIGN_MANIFEST)" || \
		(echo "H3_FAULT_CAMPAIGN_MANIFEST is required" >&2; exit 2)
	@test -n "$(H3_FAULT_EVIDENCE_OUTPUT)" || \
		(echo "H3_FAULT_EVIDENCE_OUTPUT is required" >&2; exit 2)
	go run ./cmd/vela-h3-evidence build-fault-campaign \
		"$(H3_FAULT_CAMPAIGN_MANIFEST)" "$(H3_FAULT_EVIDENCE_OUTPUT)"

verify-launch:
	@test -n "$(RELEASE_BUNDLE)" || \
		(echo "RELEASE_BUNDLE is required" >&2; exit 2)
	@test -n "$(SUPPLY_CHAIN_MANIFEST)" || \
		(echo "SUPPLY_CHAIN_MANIFEST is required" >&2; exit 2)
	@test -n "$(SUPPLY_CHAIN_POLICY)" || \
		(echo "SUPPLY_CHAIN_POLICY is required" >&2; exit 2)
	@test -n "$(LAUNCH_RECEIPTS)" || \
		(echo "LAUNCH_RECEIPTS is required" >&2; exit 2)
	go run ./cmd/vela-verify-launch "$(RELEASE_BUNDLE)" \
		"$(SUPPLY_CHAIN_MANIFEST)" "$(SUPPLY_CHAIN_POLICY)" "$(LAUNCH_RECEIPTS)"

test-cnpg-failover:
	./hack/test-cnpg-failover.sh

test-cnpg-pitr:
	./hack/test-cnpg-pitr.sh

test-cross:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -exec=/usr/bin/true ./...

validate-deployment:
	kubectl kustomize deploy/control-storage >/dev/null
	kubectl kustomize deploy/vela-control >/dev/null
	kubectl kustomize deploy/stage-worker >/dev/null
	kubectl kustomize deploy/fleet-controller >/dev/null
	kubectl kustomize deploy/observability >/dev/null
	@test -s deploy/node-agent/vela-node-agent.service
	@test -s deploy/node-agent/README.md
	@rg -q "VELA_NODE_AGENT_CONTROLLERS_FILE" deploy/node-agent/README.md cmd/vela-node-agent/main.go
	@rg -q "VELA_NODE_AGENT_WORKER_QUOTA_SOCKET" deploy/node-agent/README.md cmd/vela-node-agent/main.go
	@go test ./internal/deploymentcontract -run 'TestVelaControl' -count=1
	@go test ./internal/deploymentcontract -run 'TestStageWorker' -count=1
	@go test ./internal/deploymentcontract -run 'TestFleet' -count=1
	@go test ./internal/deploymentcontract -run '^TestRenderedRootMaterializersUsePinnedBusyBoxImage$$' -count=1
	@go test ./internal/deploymentcontract -run 'TestObservability' -count=1

verify-generated: generate
	git diff --exit-code -- api/gen internal/store/sqlc proto/gen
	@test -z "$$(git status --porcelain --untracked-files=all -- api/gen internal/store/sqlc proto/gen)" || \
		(git status --short --untracked-files=all -- api/gen internal/store/sqlc proto/gen; exit 1)

verify: verify-generated
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) test-cross
	$(MAKE) validate-deployment
