FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
COPY vendor/ vendor/
COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -o /chronicle ./cmd/chronicle

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /chronicle /usr/local/bin/chronicle
EXPOSE 8700
ENTRYPOINT ["chronicle"]
