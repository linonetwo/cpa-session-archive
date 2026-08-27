ARG GO_IMAGE=golang:1.24.13-bookworm@sha256:1a6d4452c65dea36aac2e2d606b01b4a029ec90cc1ae53890540ce6173ea77ac
ARG RUNTIME_IMAGE=gcr.io/distroless/base-nossl-debian13:nonroot@sha256:5cab74e7f8a5e7c5f1c8a9e6268b1f352f053c36c656f493308340bcecbc636c
FROM ${GO_IMAGE} AS build
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN mkdir -p /out && \
    CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false -tags cshared -buildmode=c-shared -o /out/cpa-session-archive.so ./cmd/cpa-session-archive && \
    CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false -o /out/cpa-session-collector ./cmd/cpa-session-collector && \
    CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false -o /out/cpa-session-identity-migrate ./cmd/cpa-session-identity-migrate && \
    CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false -o /out/cpa-session-archive-backup ./cmd/cpa-session-archive-backup && \
    install -d -m 0770 -o 65532 -g 65532 /out/data
FROM ${RUNTIME_IMAGE}
LABEL org.opencontainers.image.source="https://github.com/linonetwo/cpa-session-archive"
LABEL org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build --chmod=0555 /out/cpa-session-archive.so /plugin/cpa-session-archive.so
COPY --from=build --chmod=0555 /out/cpa-session-collector /usr/local/bin/cpa-session-collector
COPY --from=build --chmod=0555 /out/cpa-session-identity-migrate /usr/local/bin/cpa-session-identity-migrate
COPY --from=build --chmod=0555 /out/cpa-session-archive-backup /usr/local/bin/cpa-session-archive-backup
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/cpa-session-collector"]
