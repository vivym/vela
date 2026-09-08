#!/usr/bin/env bash
# Negative kernel experiment; never a startup gate or Launch Receipt.
set -euo pipefail
mm_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mm_evidence="${VELA_MM_WITNESS_EVIDENCE:-$(mktemp -d /tmp/vela-mm-witness.XXXXXX)}"
mm_builder='golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1'
[[ "$mm_evidence" = /* ]] || exit 2
mkdir -p "$mm_evidence/image"
cp "$mm_repo/hack/experiments/runtime-mm-witness/"{probe.c,check.py} "$mm_evidence/image/"
cat > "$mm_evidence/image/Dockerfile" <<DOCKERFILE
FROM $mm_builder
COPY probe.c check.py /
RUN cc -std=c11 -O2 -Wall -Wextra -Werror -static -fno-pie -no-pie -DIMAGE_LABEL='"A"' /probe.c -o /probe-a && \\
    cc -std=c11 -O2 -Wall -Wextra -Werror -static -fno-pie -no-pie -DIMAGE_LABEL='"B"' /probe.c -o /probe-b
ENTRYPOINT ["python3", "/check.py"]
DOCKERFILE
git -C "$mm_repo" rev-parse HEAD > "$mm_evidence/baseline.txt"
docker version > "$mm_evidence/docker-version.txt"
docker build --network none --progress plain --iidfile "$mm_evidence/image.txt" "$mm_evidence/image" > "$mm_evidence/build.log" 2>&1
mm_image="$(cat "$mm_evidence/image.txt")"
# Private PID namespace; no host paths, network or GPU in the experiment.
# SYS_PTRACE permits root observation of the deliberately non-dumpable target.
docker run --rm --init --network none --cap-add SYS_PTRACE --security-opt seccomp=unconfined \
  --cpus 1 --memory 256m --pids-limit 32 "$mm_image" > "$mm_evidence/results.json" 2> "$mm_evidence/native.stderr"
docker run --rm --network none --entrypoint sha256sum "$mm_image" \
  /probe-a /probe-b /probe.c /check.py > "$mm_evidence/files.sha256"
mm_without_ptrace=0
docker run --rm --init --network none --cap-drop SYS_PTRACE --security-opt seccomp=unconfined \
  --cpus 1 --memory 256m --pids-limit 32 "$mm_image" > "$mm_evidence/no-ptrace.log" 2>&1 || mm_without_ptrace=$?
echo "$mm_without_ptrace" > "$mm_evidence/no-ptrace-exit-code.txt"
[[ "$mm_without_ptrace" = 1 ]] || exit 1
rg -q "PermissionError: .*Permission denied: 'exe'" "$mm_evidence/no-ptrace.log" || exit 1
echo "Retained-mm negative experiment passed: $mm_evidence"
