# One Dockerfile for all four Go services. They share a module and differ
# only by which cmd/ package is the entrypoint, so a build arg beats four
# near-identical files that would drift apart.
FROM golang:1.26-alpine AS build
ARG SERVICE
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN test -n "$SERVICE" || (echo "SERVICE build arg is required" && exit 1)
RUN CGO_ENABLED=0 go build -o /out/service ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/service /service
EXPOSE 8080
ENTRYPOINT ["/service"]
