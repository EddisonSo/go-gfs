# Directories
PROTO_DIR := proto
OUT_DIR := gen

# Protobuf
PROTOC := protoc
PROTO_FILES := $(wildcard $(PROTO_DIR)/**/*.proto)
PROTOC_GEN_GO := $(shell which protoc-gen-go)
PROTOC_GEN_GO_GRPC := $(shell which protoc-gen-go-grpc)

# Main build targets
all: proto chunkserver master client

chunkserver:
	mkdir -p build
	go build -o ./build/chunkserver ./cmd/chunkserver/

master:
	mkdir -p build
	go build -o ./build/master ./cmd/master/

client:
	mkdir -p build
	go build -o ./build/client ./cmd/client/

proto: check-tools
	@echo "Generating protobuf Go code..."
	@mkdir -p $(OUT_DIR)
	$(PROTOC) \
		-I $(PROTO_DIR) \
		--go_out=$(OUT_DIR) \
		--go-grpc_out=$(OUT_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

clean:
	rm -rf build $(OUT_DIR)

check-tools:
	@if [ -z "$(PROTOC_GEN_GO)" ] || [ -z "$(PROTOC_GEN_GO_GRPC)" ]; then \
		echo "Error: protoc-gen-go or protoc-gen-go-grpc not found in PATH."; \
		echo "Install with:"; \
		echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"; \
		echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"; \
		exit 1; \
	fi

.PHONY: all chunkserver master client proto clean check-tools

