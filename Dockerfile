# pgcoalesce — static Go binary on distroless (DRAFT; not built here: no container runtime on the build box)
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY src/go.mod src/go.sum ./
RUN go mod download
COPY src/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pgcoalesce .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pgcoalesce /usr/local/bin/pgcoalesce
EXPOSE 6432 9187
USER nonroot
ENTRYPOINT ["/usr/local/bin/pgcoalesce", "-config", "/etc/pgcoalesce/pgcoalesce.ini"]
