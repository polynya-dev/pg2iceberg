# Build the benchmark HTTP shim. Runs the Go toolchain natively on the
# build host (BUILDPLATFORM) and cross-compiles to the target arch —
# emulating an amd64 Go toolchain under QEMU on Apple Silicon segfaults,
# so we never do that. CGO is off, so cross-compilation is trivial.
FROM --platform=$BUILDPLATFORM golang:1.22-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /shim .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /shim /shim
EXPOSE 8080
ENTRYPOINT ["/shim"]
