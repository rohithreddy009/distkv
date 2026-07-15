FROM golang:1.26.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /distkv ./cmd/distkv && \
    CGO_ENABLED=0 go build -o /distkv-cli ./cmd/distkv-cli

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /distkv /usr/local/bin/distkv
COPY --from=build /distkv-cli /usr/local/bin/distkv-cli
EXPOSE 7001 8001 9001
ENTRYPOINT ["distkv"]
