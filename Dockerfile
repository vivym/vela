# syntax=docker/dockerfile:1.20@sha256:26147acbda4f14c5add9946e2fd2ed543fc402884fd75146bd342a7f6271dc1d

# Bake owns the pinned identities; scratch is only a valid lint sentinel.
ARG GO_BASE=scratch
ARG DEBIAN_BASE=scratch
ARG H3_RUNTIME_BASE=scratch

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
	      vela-model-runtime \
	      vela-release-artifacts \
	      vela-stage-worker-agent; do \
      go build -mod=readonly -trimpath -buildvcs=false -ldflags='-buildid= -s -w' \
        -o "/out/${binary}" "./cmd/${binary}"; \
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

FROM scratch AS vela-stage-worker-agent
ARG RELEASE_REVISION
LABEL org.opencontainers.image.source="https://github.com/vivym/vela" \
      org.opencontainers.image.revision="${RELEASE_REVISION}" \
      org.opencontainers.image.title="vela-stage-worker-agent"
COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=go-builder --chmod=0555 /out/vela-stage-worker-agent /usr/local/bin/vela-stage-worker-agent
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/vela-stage-worker-agent"]

FROM scratch AS vela-model-runtime-base
ARG RELEASE_REVISION
LABEL org.opencontainers.image.source="https://github.com/vivym/vela" \
      org.opencontainers.image.revision="${RELEASE_REVISION}" \
      org.opencontainers.image.title="vela-model-runtime"
COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=go-builder --chmod=0555 /out/vela-model-runtime /usr/local/bin/vela-model-runtime
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/vela-model-runtime"]

FROM go-builder AS h3-runtime-command-verifier
ARG H3_ENCODER_SHA256
ARG H3_DIT_SHA256
ARG H3_VAE_DECODER_SHA256
COPY --from=h3_runtime_commands --chmod=0555 /h3-encoder /h3-runtime-commands/h3-encoder
COPY --from=h3_runtime_commands --chmod=0555 /h3-dit /h3-runtime-commands/h3-dit
COPY --from=h3_runtime_commands --chmod=0555 /h3-vae-decoder /h3-runtime-commands/h3-vae-decoder
RUN /out/vela-release-artifacts verify-h3-runtime-commands \
      /h3-runtime-commands \
      "${H3_ENCODER_SHA256}" \
      "${H3_DIT_SHA256}" \
      "${H3_VAE_DECODER_SHA256}"

FROM ${H3_RUNTIME_BASE} AS vela-h3-stage-runtime
ARG RELEASE_REVISION
ARG H3_RUNTIME_BASE
ARG H3_ENCODER_SHA256
ARG H3_DIT_SHA256
ARG H3_VAE_DECODER_SHA256
LABEL org.opencontainers.image.source="https://github.com/vivym/vela" \
      org.opencontainers.image.revision="${RELEASE_REVISION}" \
      org.opencontainers.image.title="vela-h3-stage-runtime" \
      vela.ai.h3-runtime-base="${H3_RUNTIME_BASE}" \
      vela.ai.h3-encoder.sha256="${H3_ENCODER_SHA256}" \
      vela.ai.h3-dit.sha256="${H3_DIT_SHA256}" \
      vela.ai.h3-vae-decoder.sha256="${H3_VAE_DECODER_SHA256}"
COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=go-builder --chmod=0555 /out/vela-model-runtime /usr/local/bin/vela-model-runtime
COPY --from=h3-runtime-command-verifier --chmod=0555 /h3-runtime-commands/h3-encoder /opt/vela/bin/h3-encoder
COPY --from=h3-runtime-command-verifier --chmod=0555 /h3-runtime-commands/h3-dit /opt/vela/bin/h3-dit
COPY --from=h3-runtime-command-verifier --chmod=0555 /h3-runtime-commands/h3-vae-decoder /opt/vela/bin/h3-vae-decoder
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/vela-model-runtime"]
CMD []
