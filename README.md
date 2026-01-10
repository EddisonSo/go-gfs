# Go-GFS: Distributed File System

A simplified Google File System (GFS) implementation in Go, featuring append-only writes, replication, and two-phase commit for data durability.

## Status

**Current:** Data persistence layer is complete and functional. Master server is not yet implemented.

## Key Features

- **Append-only writes** - No random writes or overwrites
- **Replication Factor 3** - Fault-tolerant storage across 3 chunkservers
- **Two-Phase Commit (2PC)** - Atomic persistence with quorum-based writes (2/3)
- **Chunk-based storage** - 64MB chunks with offset allocation
- **Dual communication planes** - TCP for clients (port 8080), gRPC for replication (port 8081)

## Architecture

```
Client (TCP :8080)
    ↓
Primary Chunkserver
    ↓ (gRPC :8081)
Replica Chunkservers (RF=3)
```

### Write Flow (2PC)

1. **READY Phase:** Client sends data → Primary allocates offset → Data replicated to all replicas (in-memory staging)
2. **COMMIT Phase:** Primary waits for quorum (2/3) → Sends COMMIT signal → All replicas persist to disk with fsync

## Quick Start

### Build

```bash
make all           # Build chunkserver and tester
make proto         # Regenerate protobuf files
```

### Run with Docker

```bash
# Start 3-node cluster
docker compose up --build

# Run test client (in another terminal)
./build/tester

# Verify data written to all replicas
cat data/chunkserver1/1234
cat data/chunkserver2/1234
cat data/chunkserver3/1234

# Clean up
docker compose down
```

### Run Locally

```bash
# Terminal 1: Primary
./build/chunkserver -p 8080 -r 8081 -h localhost -d tmp/cs1 -id cs1

# Terminal 2: Replica 2
./build/chunkserver -p 8082 -r 8083 -h localhost -d tmp/cs2 -id cs2

# Terminal 3: Replica 3
./build/chunkserver -p 8084 -r 8085 -h localhost -d tmp/cs3 -id cs3

# Terminal 4: Test
./build/tester
```

## Project Structure

```
cmd/
  chunkserver/      - Chunkserver entry point
  tester/           - Test client for data persistence
  master/           - Master server (not implemented)

internal/chunkserver/
  dataplane/        - TCP server for client connections (:8080)
  replicationplane/ - gRPC server for replica communication (:8081)
  stagedchunk/      - In-memory staged data + disk persistence
  fanoutcoordinator/ - Parallel data replication

proto/              - gRPC service definitions
data/               - Persistent storage (Docker volumes)
```

## What's Implemented

- ✅ Data replication across 3 chunkservers
- ✅ Quorum-based writes (2/3 must confirm)
- ✅ Atomic commits with fsync for durability
- ✅ gRPC for internal communication
- ✅ Offset allocation for append operations

## What's NOT Implemented

- ❌ Master server (metadata, namespace, health checks)
- ❌ Client library (high-level API)
- ❌ Lease management
- ❌ Checksums for data integrity
- ❌ Garbage collection
- ❌ Re-replication on failure

## Documentation

See [CLAUDE.md](CLAUDE.md) for comprehensive developer documentation including:
- Detailed architecture and component descriptions
- Protocol specifications
- Testing guide
- Troubleshooting
- Code patterns and implementation notes
