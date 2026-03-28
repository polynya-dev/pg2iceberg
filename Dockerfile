FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /pg2iceberg ./cmd/pg2iceberg

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /pg2iceberg /usr/local/bin/pg2iceberg

# Reduce GC overhead: only GC when approaching the memory limit, not on every 2x heap growth.
# GOMEMLIMIT should be set to ~80% of container memory at deploy time (e.g. GOMEMLIMIT=1600MiB for 2GiB container).
ENV GOGC=off
ENV GOMEMLIMIT=1600MiB

ENTRYPOINT ["pg2iceberg"]
