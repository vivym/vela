# syntax=docker/dockerfile:1.20@sha256:26147acbda4f14c5add9946e2fd2ed543fc402884fd75146bd342a7f6271dc1d

# Bake owns the pinned identities; scratch is only a valid lint sentinel.
ARG GO_BASE=scratch
ARG PYTHON_BASE=scratch
ARG DEBIAN_BASE=scratch
ARG UV_BASE=scratch

FROM ${GO_BASE} AS go-builder
ARG GOPROXY=https://proxy.golang.org,direct
WORKDIR /src
ENV CGO_ENABLED=0 \
    GOARCH=amd64 \
    GOOS=linux \
    GOPROXY="${GOPROXY}" \
    GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    go mod download
COPY api ./api
COPY cmd ./cmd
COPY internal ./internal
COPY proto ./proto
ARG RELEASE_REVISION
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    test -n "${RELEASE_REVISION}" && \
    mkdir -p /out && \
    for binary in \
      vela-control \
      vela-artifact-validator \
      vela-fleet-controller \
      vela-worker-agent \
      vela-release-artifacts; do \
      go build -mod=readonly -trimpath -buildvcs=false -ldflags='-buildid= -s -w' \
        -o "/out/${binary}" "./cmd/${binary}"; \
    done

FROM go-builder AS lab-go-builder
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    mkdir -p /out-lab && \
    for binary in \
      vela-lab-assets \
      vela-lab-bootstrap \
      vela-lab-smoke; do \
      go build -mod=readonly -trimpath -buildvcs=false -ldflags='-buildid= -s -w' \
        -o "/out-lab/${binary}" "./cmd/${binary}"; \
    done

FROM ${DEBIAN_BASE} AS ffprobe-builder
ENV DEBIAN_FRONTEND=noninteractive \
    SOURCE_DATE_EPOCH=315532800
RUN apt-get update && \
    apt-get install --yes --no-install-recommends \
      build-essential=12.9 \
      musl-tools=1.2.3-1 \
      nasm=2.16.01-1 \
      xz-utils=5.4.1-1+deb12u1 && \
    rm -rf /var/lib/apt/lists/*
ADD --checksum=sha256:05ee0b03119b45c0bdb4df654b96802e909e0a752f72e4fe3794f487229e5a41 \
    https://ffmpeg.org/releases/ffmpeg-8.0.1.tar.xz /tmp/ffmpeg-8.0.1.tar.xz
RUN printf '%s  %s\n' \
      '05ee0b03119b45c0bdb4df654b96802e909e0a752f72e4fe3794f487229e5a41' \
      '/tmp/ffmpeg-8.0.1.tar.xz' | sha256sum --check --strict && \
    tar --extract --file=/tmp/ffmpeg-8.0.1.tar.xz --directory=/tmp && \
    cd /tmp/ffmpeg-8.0.1 && \
    ./configure \
      --cc=musl-gcc \
      --disable-everything \
      --disable-autodetect \
      --disable-doc \
      --disable-network \
      --disable-shared \
      --enable-static \
      --extra-ldflags=-static \
      --enable-decoder=h264 \
      --enable-decoder=webp \
      --enable-demuxer=image_webp_pipe \
      --enable-demuxer=mov \
      --enable-ffprobe \
      --enable-parser=h264 \
      --enable-parser=webp \
      --enable-protocol=fd \
      --enable-protocol=file && \
    make --jobs="$(nproc)" ffprobe && \
    install --mode=0555 ffprobe /out-ffprobe

FROM ${UV_BASE} AS uv-binary

FROM ${PYTHON_BASE} AS runner-builder
COPY --from=uv-binary /uv /usr/local/bin/uv
WORKDIR /src/runner
ENV SOURCE_DATE_EPOCH=315532800 \
    UV_NO_PROGRESS=true \
    UV_PROJECT_ENVIRONMENT=/opt/vela/venv
COPY runner/pyproject.toml runner/uv.lock ./
COPY runner/src ./src
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    uv sync --frozen --no-dev --no-editable && \
    test -x /opt/vela/venv/bin/vela-h3-runner

FROM ${DEBIAN_BASE} AS h3-backend-verifier
ARG H3_BACKEND_SHA256
COPY --from=go-builder --chmod=0555 /out/vela-release-artifacts /usr/local/bin/vela-release-artifacts
COPY --from=h3_backend --chmod=0555 /h3-backend /backend-context/h3-backend
RUN /usr/local/bin/vela-release-artifacts verify-h3-backend \
      /backend-context "${H3_BACKEND_SHA256}" && \
    install --mode=0555 /backend-context/h3-backend /verified-h3-backend

FROM scratch AS vela-control
ARG RELEASE_REVISION
LABEL org.opencontainers.image.source="https://github.com/vivym/vela" \
      org.opencontainers.image.revision="${RELEASE_REVISION}" \
      org.opencontainers.image.title="vela-control"
COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=go-builder --chmod=0555 /out/vela-control /usr/local/bin/vela-control
COPY --from=go-builder --chmod=0555 /out/vela-artifact-validator /usr/local/bin/vela-artifact-validator
COPY --from=ffprobe-builder --chmod=0555 /out-ffprobe /usr/bin/ffprobe
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/vela-control"]

FROM scratch AS vela-fleet-controller
ARG RELEASE_REVISION
LABEL org.opencontainers.image.source="https://github.com/vivym/vela" \
      org.opencontainers.image.revision="${RELEASE_REVISION}" \
      org.opencontainers.image.title="vela-fleet-controller"
COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=go-builder --chmod=0555 /out/vela-fleet-controller /usr/local/bin/vela-fleet-controller
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/vela-fleet-controller"]

FROM scratch AS vela-worker-agent
ARG RELEASE_REVISION
LABEL org.opencontainers.image.source="https://github.com/vivym/vela" \
      org.opencontainers.image.revision="${RELEASE_REVISION}" \
      org.opencontainers.image.title="vela-worker-agent"
COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=go-builder --chmod=0555 /out/vela-worker-agent /usr/local/bin/vela-worker-agent
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/vela-worker-agent"]

FROM vela-control AS vela-lab-control
LABEL vela.ai.build-kind="noncanonical-lab" \
      vela.ai.environment="non-production-lab"

FROM vela-worker-agent AS vela-lab-worker-agent
LABEL vela.ai.build-kind="noncanonical-lab" \
      vela.ai.environment="non-production-lab"

FROM ${DEBIAN_BASE} AS vela-lab-bootstrap
ARG RELEASE_REVISION
LABEL org.opencontainers.image.source="https://github.com/vivym/vela" \
      org.opencontainers.image.revision="${RELEASE_REVISION}" \
      org.opencontainers.image.title="vela-lab-bootstrap" \
      vela.ai.build-kind="noncanonical-lab" \
      vela.ai.environment="non-production-lab"
COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=lab-go-builder --chmod=0555 /out-lab/vela-lab-bootstrap /usr/local/bin/vela-lab-bootstrap
COPY --from=lab-go-builder --chmod=0555 /out-lab/vela-lab-smoke /usr/local/bin/vela-lab-smoke
COPY --chmod=0444 db/bootstrap/roles.sql /opt/vela/share/db/bootstrap/roles.sql
COPY --chmod=0444 db/migrations /opt/vela/share/db/migrations
RUN find /opt/vela -type d -exec chmod 0555 {} + && \
    find /opt/vela/share/db -type f -exec chmod 0444 {} +
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/vela-lab-bootstrap"]

FROM ${PYTHON_BASE} AS vela-h3-runner
ARG RELEASE_REVISION
ARG H3_BACKEND_SHA256
LABEL org.opencontainers.image.source="https://github.com/vivym/vela" \
      org.opencontainers.image.revision="${RELEASE_REVISION}" \
      org.opencontainers.image.title="vela-h3-runner" \
      vela.ai.h3-backend.sha256="${H3_BACKEND_SHA256}"
ENV PATH="/opt/vela/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
COPY --from=runner-builder /opt/vela/venv /opt/vela/venv
COPY --from=h3-backend-verifier --chmod=0555 /verified-h3-backend /opt/vela/bin/h3-backend
USER 10001:10001
ENTRYPOINT ["/opt/vela/venv/bin/vela-h3-runner"]
