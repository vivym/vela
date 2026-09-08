#!/usr/bin/env bash
set -euo pipefail
baseline_dir=/tmp/vela-startup-validation.syiSMk/task-state/baseline
baseline_repo=/Users/viv/projs/vela
baseline_builder='golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1'
mkdir -p "$baseline_dir/image/rootfs"
git -C "$baseline_repo" show 344e0f2:internal/nodeagent/runtime_task_launch_linux.go > "$baseline_dir/runtime_task_launch_linux.go"
python3 - "$baseline_repo" "$baseline_dir" <<'PY'
import json, pathlib, sys
repo, dest = map(pathlib.Path, sys.argv[1:])
test = repo / 'internal/nodeagent/runtime_task_launch_integration_linux_test.go'
text = test.read_text()
assert text.count(' || launch.DaemonStatePath != state') == 1
text = text.replace(' || launch.DaemonStatePath != state', '')
(dest / test.name).write_text(text)
mapping = {'Replace': {
    '/workspace/internal/nodeagent/runtime_task_launch_linux.go': '/evidence/runtime_task_launch_linux.go',
    '/workspace/internal/nodeagent/runtime_task_launch_integration_linux_test.go': '/evidence/runtime_task_launch_integration_linux_test.go',
}}
(dest / 'overlay.json').write_text(json.dumps(mapping, indent=2) + '\n')
PY
docker run --rm --network none --cpus 4 --memory 4g --pids-limit 512 \
  -v "$baseline_repo:/workspace:ro" -v /Users/viv/go/pkg/mod:/go/pkg/mod:ro \
  -v "$baseline_dir:/evidence" -v /tmp/vela-startup-validation.syiSMk/custody-native-race/go-cache:/build-cache \
  -w /workspace -e GOCACHE=/build-cache -e GOPROXY=off -e CGO_ENABLED=1 \
  "$baseline_builder" go test -race -tags=integration -overlay=/evidence/overlay.json -c \
  -ldflags '-linkmode external -extldflags=-static' -o /evidence/image/rootfs/nodeagent.test ./internal/nodeagent \
  > "$baseline_dir/build.log" 2>&1
cp -R /tmp/vela-startup-validation.syiSMk/task-state/native-verified/image/rootfs/usr "$baseline_dir/image/rootfs/"
cp /tmp/vela-startup-validation.syiSMk/task-state/native-verified/image/Dockerfile "$baseline_dir/image/Dockerfile"
baseline_image="$(docker build --network none -q "$baseline_dir/image")"
echo "$baseline_image" > "$baseline_dir/image.txt"
baseline_status=0
docker run --rm --network none --privileged --cgroupns private --cpus 4 --memory 4g --pids-limit 512 \
  -e VELA_TEST_CONTAINERD_SANDBOX=1 "$baseline_image" \
  -test.run='^TestRuntimeCallerContainerCRI$' -test.count=1 -test.v -test.timeout=3m \
  > "$baseline_dir/native.log" 2>&1 || baseline_status=$?
echo "$baseline_status" > "$baseline_dir/exit-code.txt"
[[ "$baseline_status" == 1 ]]
[[ "$(rg -c 'root-owned copy of the live task.s exact config and PID was accepted' "$baseline_dir/native.log")" == 2 ]]
if rg -q -- '--- SKIP:|WARNING: DATA RACE' "$baseline_dir/native.log"; then exit 1; fi
echo 'Baseline rejected by both copied-state-root regressions as expected'
