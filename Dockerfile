FROM golang:1.24 AS builder
WORKDIR /app

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Copy go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Install protobuf Go plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

# Ensure the installed binaries are in PATH
ENV PATH="${PATH}:/go/bin"

# Copy source code
COPY . .

# Generate unique build ID and timestamp at build time, save to file for labels
RUN BUILD_ID=$(head -c 16 /dev/urandom | md5sum | head -c 8) && \
    BUILD_TIME=$(date -u '+%Y-%m-%d_%H:%M:%S') && \
    echo "$BUILD_ID" > /tmp/build_id && \
    echo "$BUILD_TIME" > /tmp/build_time && \
    make proto && \
    BUILD_ID=$BUILD_ID BUILD_TIME=$BUILD_TIME make all

FROM debian:bookworm-slim
WORKDIR /root/

# Copy build info for labels
COPY --from=builder /tmp/build_id /tmp/build_time /tmp/

# Install netcat for healthchecks
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/build/chunkserver ./chunkserver
COPY --from=builder /app/build/master ./master
COPY --from=builder /app/build/client ./client

EXPOSE 8080
EXPOSE 8081
EXPOSE 9000

CMD ["./chunkserver", "-h", "0.0.0.0"]
