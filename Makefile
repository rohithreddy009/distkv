PROTOC      := protoc
GOBIN       := $(shell go env GOPATH)/bin
PROTO_FLAGS := --plugin=protoc-gen-go=$(GOBIN)/protoc-gen-go \
               --plugin=protoc-gen-go-grpc=$(GOBIN)/protoc-gen-go-grpc

.PHONY: proto build test bench clean

proto:
	$(PROTOC) $(PROTO_FLAGS) \
		--go_out=. --go_opt=module=github.com/rohithreddy/distkv \
		--go-grpc_out=. --go-grpc_opt=module=github.com/rohithreddy/distkv \
		proto/raft.proto proto/kv.proto

build:
	go build ./...

test:
	go test -race ./...

bench:
	go run ./cmd/bench

clean:
	rm -rf bin/
