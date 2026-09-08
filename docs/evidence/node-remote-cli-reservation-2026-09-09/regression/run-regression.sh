#!/usr/bin/env bash
set -euo pipefail
regression_repo=/Users/viv/projs/vela
regression_evidence=/tmp/vela-startup-validation.syiSMk/remote-cli-reservation/regression
regression_modules="$(go env GOMODCACHE)"
regression_builder='golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1'
docker run --rm --network none --cpus 4 --memory 4g --pids-limit 512 \
  -v "$regression_repo:/workspace:ro" -v "$regression_modules:/go/pkg/mod:ro" \
  -v "$regression_evidence:/evidence" -v /tmp/vela-startup-validation.syiSMk/custody-native-race/go-cache:/build-cache \
  -w /workspace -e GOCACHE=/build-cache -e GOPROXY=off -e CGO_ENABLED=1 "$regression_builder" \
  go test -race -tags=integration -overlay=/evidence/overlay.json -c \
  -ldflags '-linkmode external -extldflags=-static' -o /evidence/nodeagent.test ./internal/nodeagent > "$regression_evidence/build.log" 2>&1
regression_parent="$(cat /tmp/vela-startup-validation.syiSMk/remote-cli-reservation/native-explicit-home/image.txt)"
regression_tag="vela-remote-cli-${regression_parent:7:12}:local"
docker tag "$regression_parent" "$regression_tag"
printf 'FROM %s\nCOPY nodeagent.test /nodeagent.test\n' "$regression_tag" > "$regression_evidence/Dockerfile"
regression_image="$(docker build --network none -q "$regression_evidence")"
echo "$regression_image" > "$regression_evidence/image.txt"
shasum -a 256 "$regression_evidence/nodeagent.test" > "$regression_evidence/binaries.sha256"
regression_exit=0
docker run --rm --network none --privileged --cgroupns private --cpus 4 --memory 4g --pids-limit 512 \
  -e VELA_TEST_CONTAINERD_SANDBOX=1 "$regression_image" \
  '-test.run=^TestRuntimeCallerContainerCRI$/^remote-cli-reservation$/^(valid|hidden-env|hidden-argument)$' \
  -test.v -test.count=1 -test.timeout=1m > "$regression_evidence/native.log" 2>&1 || regression_exit=$?
echo "$regression_exit" > "$regression_evidence/exit-code.txt"
[[ "$regression_exit" == 1 ]]
for regression_case in hidden-env hidden-argument; do
  rg -q -- "--- FAIL: TestRuntimeCallerContainerCRI/remote-cli-reservation/$regression_case " "$regression_evidence/native.log"
done
rg -q -- '--- PASS: TestRuntimeCallerContainerCRI/remote-cli-reservation/valid ' "$regression_evidence/native.log"
if rg -q -- 'WARNING: DATA RACE|--- SKIP:' "$regression_evidence/native.log"; then exit 1; fi
echo 'Both task-vector concealment regressions reproduced; normal CLI control passed'
