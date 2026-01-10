FROM golang:1.23 AS builder
WORKDIR /app

# Install protobuf compiler first
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

# Generate protobuf files and build all binaries
RUN make proto
RUN make chunkserver
RUN make master

FROM debian:bookworm-slim
WORKDIR /root/

# Install netcat for healthchecks
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/build/chunkserver ./chunkserver
COPY --from=builder /app/build/master ./master

EXPOSE 8080
EXPOSE 8081
EXPOSE 9000

CMD ["./chunkserver", "-h", "0.0.0.0"]
