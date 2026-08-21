FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION}" -o /slipstream ./cmd/slipstream

FROM gcr.io/distroless/static-debian12
COPY --from=build /slipstream /slipstream
USER 65532:65532
ENTRYPOINT ["/slipstream"]
CMD ["run", "-config", "/etc/slipstream/slipstream.yaml"]
