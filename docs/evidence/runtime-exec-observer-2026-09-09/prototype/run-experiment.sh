#!/usr/bin/env bash
# Evaluate a creation-time syscall observer, not a production launch grant.
set -euo pipefail
observer_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
observer_evidence="${VELA_EXEC_OBSERVER_EVIDENCE:-$(mktemp -d /tmp/vela-exec-observer.XXXXXX)}"
observer_builder='golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1'
[[ "$observer_evidence" = /* ]] || exit 2
mkdir -p "$observer_evidence/image"
cp "$observer_repo/hack/experiments/runtime-exec-observer/"{observer.c,probe.c,check.py} "$observer_evidence/image/"
cp "$observer_repo/hack/experiments/runtime-exec-observer/go-probe.go.txt" "$observer_evidence/image/go-probe.go"
cat > "$observer_evidence/image/Dockerfile" <<DOCKERFILE
FROM $observer_builder
COPY observer.c probe.c go-probe.go check.py /
RUN cc -std=c11 -O2 -Wall -Wextra -Werror -static /observer.c -o /exec-observer && \\
    cc -std=c11 -O2 -Wall -Wextra -Werror -static -DDISABLE_CLONE_GUARD /observer.c -o /exec-observer-disabled && \\
    cc -std=c11 -O2 -Wall -Wextra -Werror -static -DDISABLE_EXITKILL /observer.c -o /exec-observer-no-exitkill && \\
    cc -std=c11 -O2 -Wall -Wextra -Werror -static -pthread /probe.c -o /exec-probe && \\
    CGO_ENABLED=0 GOPROXY=off go build -o /go-probe /go-probe.go
ENTRYPOINT ["python3", "/check.py"]
DOCKERFILE
git -C "$observer_repo" rev-parse HEAD > "$observer_evidence/baseline.txt"
docker version > "$observer_evidence/docker-version.txt"
docker build --network none --progress plain --iidfile "$observer_evidence/image.txt" "$observer_evidence/image" > "$observer_evidence/build.log" 2>&1
observer_image="$(cat "$observer_evidence/image.txt")"
# Private processes only: no host mounts, network or GPU. The observer drops the
# targets to UID/GID 65534; SYS_PTRACE supports their non-dumpable state.
docker run --rm --init --network none --cap-add SYS_PTRACE --security-opt seccomp=unconfined \
  --cpus 2 --memory 512m --pids-limit 256 "$observer_image" > "$observer_evidence/results.json" 2> "$observer_evidence/native.stderr"
docker run --rm --network none --entrypoint sha256sum "$observer_image" \
  /exec-observer /exec-observer-disabled /exec-observer-no-exitkill /exec-probe /go-probe /observer.c /probe.c /go-probe.go /check.py > "$observer_evidence/files.sha256"
echo "Creation-time exec observer experiment passed: $observer_evidence"
