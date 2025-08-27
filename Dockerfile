FROM golang:1.23 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
RUN apt update && apt install -y protobuf-compiler
RUN make

FROM debian:bookworm-slim
WORKDIR /root/

COPY --from=builder /app/build/chunkserver ./chunkserver

EXPOSE 8080
EXPOSE 8081

CMD ["./chunkserver", "-h", "0.0.0.0"]
