ARG GO_IMAGE=golang:1.24-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim
FROM ${GO_IMAGE} AS build
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN go test ./... && mkdir -p /out && CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildvcs=false -tags cshared -buildmode=c-shared -o /out/cpa-session-archive.so ./cmd/cpa-session-archive && CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildvcs=false -o /out/cpa-session-collector ./cmd/cpa-session-collector
FROM ${RUNTIME_IMAGE}
LABEL org.opencontainers.image.source="https://github.com/linonetwo/cpa-session-archive"
LABEL org.opencontainers.image.licenses="Apache-2.0"
RUN useradd -r -u 10001 archive
COPY --from=build /out/cpa-session-archive.so /plugin/cpa-session-archive.so
COPY --from=build /out/cpa-session-collector /usr/local/bin/cpa-session-collector
RUN chmod 0555 /plugin/cpa-session-archive.so /usr/local/bin/cpa-session-collector && mkdir /data && chown archive:archive /data
USER archive
ENTRYPOINT ["/usr/local/bin/cpa-session-collector"]
