#!/usr/bin/env bash
set -euo pipefail
regression_repo=/Users/viv/projs/vela
regression_evidence=/tmp/vela-startup-validation.syiSMk/bootstrap-consumption/regression
regression_modules="$(go env GOMODCACHE)"
regression_builder='golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1'
docker run --rm --network none --cpus 4 --memory 4g --pids-limit 512 \
  -v "$regression_repo:/workspace:ro" -v "$regression_modules:/go/pkg/mod:ro" \
  -v "$regression_evidence:/evidence" -v /tmp/vela-startup-validation.syiSMk/custody-native-race/go-cache:/build-cache \
  -w /workspace -e GOCACHE=/build-cache -e GOPROXY=off -e CGO_ENABLED=1 "$regression_builder" \
  go build -race -overlay=/evidence/overlay.json \
  -ldflags '-linkmode external -extldflags=-static' -o /evidence/vela-model-runtime ./cmd/vela-model-runtime > "$regression_evidence/build.log" 2>&1
regression_parent="$(cat /tmp/vela-startup-validation.syiSMk/bootstrap-consumption/cli-final/image.txt)"
regression_tag="vela-bootstrap-consumption-${regression_parent:7:12}:local"
docker tag "$regression_parent" "$regression_tag"
printf 'FROM %s\nCOPY vela-model-runtime /vela-model-runtime\n' "$regression_tag" > "$regression_evidence/Dockerfile"
regression_image="$(docker build --network none -q "$regression_evidence")"
echo "$regression_image" > "$regression_evidence/image.txt"
shasum -a 256 "$regression_evidence/vela-model-runtime" > "$regression_evidence/binaries.sha256"
regression_exit=0
docker run --rm --network none --cap-add SYS_ADMIN --cap-add SYS_PTRACE --security-opt seccomp=unconfined \
  --cpus 4 --memory 4g --pids-limit 256 "$regression_image" /nodeagent.test \
  -test.run '^TestRuntimeBootstrapPublicationActualCLI$' \
  -test.v -test.count=1 -test.timeout=1m > "$regression_evidence/native.log" 2>&1 || regression_exit=$?
echo "$regression_exit" > "$regression_evidence/exit-code.txt"
[[ "$regression_exit" == 1 ]]
rg -q -- '--- FAIL: TestRuntimeBootstrapPublicationActualCLI/changed-after-read ' "$regression_evidence/native.log"
for regression_case in permit alias-path; do
  rg -q -- "--- PASS: TestRuntimeBootstrapPublicationActualCLI/$regression_case " "$regression_evidence/native.log"
done
if rg -q -- 'WARNING: DATA RACE|--- SKIP:' "$regression_evidence/native.log"; then exit 1; fi
echo 'Gate-time reread rejected by the actual CLI read-before-startup regression'
