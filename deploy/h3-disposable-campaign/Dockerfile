# syntax=docker/dockerfile:1.20@sha256:26147acbda4f14c5add9946e2fd2ed543fc402884fd75146bd342a7f6271dc1d

FROM docker.io/library/golang:1.26.7-bookworm@sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2 AS builder
WORKDIR /src
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOTOOLCHAIN=local
COPY go.mod go.sum ./
ARG GOPROXY=https://proxy.golang.org,direct
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    case "$GOPROXY" in \
      https://proxy.golang.org,direct|https://goproxy.cn|https://goproxy.io|https://mirrors.aliyun.com/goproxy) ;; \
      *) echo "unsupported disposable campaign GOPROXY: $GOPROXY" >&2; exit 2 ;; \
    esac && \
    GOPROXY="$GOPROXY" go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY proto ./proto
ARG RELEASE_REVISION=unknown
RUN GOPROXY="$GOPROXY" go build -mod=readonly -trimpath -buildvcs=false -ldflags='-buildid= -s -w' \
      -o /out/vela-h3-member-campaign ./cmd/vela-h3-member-campaign

FROM scratch
ARG RELEASE_REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/vivym/vela" \
      org.opencontainers.image.revision="${RELEASE_REVISION}" \
      org.opencontainers.image.title="vela-h3-member-campaign" \
      vela.ai.evidence-scope="disposable-fake-runtime-member-transport"
COPY --from=builder --chmod=0555 /out/vela-h3-member-campaign /usr/local/bin/vela-h3-member-campaign
USER 0:0
ENTRYPOINT ["/usr/local/bin/vela-h3-member-campaign"]
