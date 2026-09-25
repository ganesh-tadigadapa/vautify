# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/vault ./cmd/vault

FROM alpine:3.22
RUN adduser -D -u 10001 vault && mkdir -p /data && chown vault:vault /data
COPY --from=build /out/vault /usr/local/bin/vault
USER vault
VOLUME /data
ENTRYPOINT ["/usr/local/bin/vault"]
