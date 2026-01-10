# Append-Only Distributed File System (GFS-style)

## Overview
This project implements a simplified Google File System (GFS)-like storage system optimized for **append-only workloads** such as logs.  
It provides a **master** for metadata management, **chunkservers** for data storage, and supports fault-tolerant appends and reads with replication.

## Core Concepts
- **Chunks:** Files are split into fixed-size chunks (default 64MB). Each chunk is replicated (RF=3).
- **Master:** Stores metadata (file → chunk mapping, replica sets, chunk lengths). Handles namespace, placement, heartbeats, and repair.
- **Chunkservers:** Store raw chunks, checksums, and handle read/write requests. They are stateless regarding file layout.
- **Append-only writes:** Data is appended atomically to the tail chunk. Once full, a new chunk is created.

## Network Architecture

### Communication Planes

```
┌─────────────────────────────────────────────────────────────┐
│                    CLIENT APPLICATION                        │
└──────────────────────┬──────────────────────────────────────┘
                       │ TCP (port 8080) - Data Plane
                       │ Custom protocol with JWT auth
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  PRIMARY CHUNKSERVER (CS1)                   │
│  - Receives client data on port 8080                         │
│  - Allocates offset atomically                               │
│  - Coordinates 2PC via gRPC on port 8081                     │
└──────┬────────────────────────────────────────────┬─────────┘
       │ gRPC (port 8081) - Replication Plane       │
       │ Replicate() streaming RPC                  │
       ▼                                            ▼
┌──────────────────┐                        ┌──────────────────┐
│  REPLICA CS2     │                        │  REPLICA CS3     │
│  - Port 8083     │                        │  - Port 8085     │
│  - Stages data   │                        │  - Stages data   │
│  - Waits commit  │                        │  - Waits commit  │
└──────────────────┘                        └──────────────────┘
       ▲                                            ▲
       │ gRPC RecvCommit() unary RPC               │
       └────────────────────┬───────────────────────┘
                            │
                   Primary sends COMMIT
```

**Data Plane (Port 8080):**
- Protocol: Custom TCP
- Purpose: Client ↔ Primary communication
- Handles: Client uploads, JWT authentication, offset allocation

**Replication Plane (Port 8081):**
- Protocol: gRPC (Replicator service)
- Purpose: Chunkserver ↔ Chunkserver communication
- Handles: Data replication, 2PC coordination (READY/COMMIT)

## Write Path (Append) - Network Call Sequence

### Complete Flow with Network Calls

```
CLIENT                PRIMARY (CS1)           REPLICA CS2           REPLICA CS3
  │                        │                       │                     │
  │ (1) TCP Connect        │                       │                     │
  │───────────────────────>│                       │                     │
  │      :8080             │                       │                     │
  │                        │                       │                     │
  │ (2) Send Request       │                       │                     │
  │  [Action|JWT|Data]     │                       │                     │
  │───────────────────────>│                       │                     │
  │                        │                       │                     │
  │                        │ (3) Allocate offset   │                     │
  │                        │     Create StagedChunk│                     │
  │                        │                       │                     │
  │ (4) Response: Offset   │                       │                     │
  │<───────────────────────│                       │                     │
  │       [uint64]         │                       │                     │
  │                        │                       │                     │
  │ (5) Stream Data        │                       │                     │
  │───────────────────────>│ (6) gRPC Replicate()  │                     │
  │                        │──────────────────────>│                     │
  │                        │       :8083           │                     │
  │                        │                       │                     │
  │                        │ (7) gRPC Replicate()  │                     │
  │                        │───────────────────────────────────────────>│
  │                        │                       │       :8085         │
  │                        │                       │                     │
  │                        │   Stream: Metadata    │                     │
  │                        │   + Data Frames       │   Stream: Metadata  │
  │                        │                       │   + Data Frames     │
  │                        │                       │                     │
  │                        │ (8) Response: Success │                     │
  │                        │<──────────────────────│                     │
  │                        │   [Calls Ready()]     │                     │
  │                        │                       │                     │
  │                        │ (9) Response: Success │                     │
  │                        │<───────────────────────────────────────────│
  │                        │   [Calls Ready()]     │                     │
  │                        │                       │                     │
  │ (6) CloseWrite (EOF)   │ (10) Check Quorum     │                     │
  │───────────────────────>│   2/3 ready? YES!     │                     │
  │                        │                       │                     │
  │                        │──── PHASE 2: COMMIT ───────────────────────│
  │                        │                       │                     │
  │                        │ (11) RecvCommit(opID) │                     │
  │                        │──────────────────────>│                     │
  │                        │                       │ (12) Commit()       │
  │                        │                       │  Write to disk      │
  │                        │                       │  fsync()            │
  │                        │                       │                     │
  │                        │ (13) RecvCommit(opID) │                     │
  │                        │───────────────────────────────────────────>│
  │                        │                       │  (14) Commit()      │
  │                        │                       │   Write to disk     │
  │                        │                       │   fsync()           │
  │                        │                       │                     │
  │                        │ (15) Response: OK     │                     │
  │                        │<──────────────────────│                     │
  │                        │                       │                     │
  │                        │ (16) Response: OK     │                     │
  │                        │<───────────────────────────────────────────│
  │                        │                       │                     │
  │                        │ (17) Local Commit()   │                     │
  │                        │  Write to disk, fsync │                     │
  │                        │                       │                     │
  │ (18) Response: Success │                       │                     │
  │<───────────────────────│                       │                     │
  │       [byte = 1]       │                       │                     │
  │                        │                       │                     │
```

### Two-Phase Commit Protocol Details

#### Phase 1: READY (Data Replication)

1. **Client → Primary:** Send data via TCP (port 8080)
   - Format: `[Action(4B)] [JWT_len(4B)] [JWT(NB)] [Data(MB)]`

2. **Primary:** Allocate offset, create StagedChunk (status=READY)
   - Atomic offset allocation per chunk
   - In-memory staging buffer created

3. **Primary → Replicas:** Stream data via gRPC `Replicate()` (port 8081)
   - First frame: ReplicationMetadata (opID, chunkHandle, offset, length)
   - Subsequent frames: ReplicationData (streaming chunks)

4. **Replicas:** Create StagedChunk, buffer data (status=READY)
   - Store in ChunkStagingTrackingService by opID
   - Data buffered in memory, NOT on disk yet

5. **Replicas → Primary:** Return success, call `Ready()`
   - Increments ready counter in primary's StagedChunk

6. **Primary:** Wait for quorum (2/3 replicas ready)
   - Polls `IsQuorumReady()` until satisfied or timeout (10s)

#### Phase 2: COMMIT (Data Persistence)

7. **Primary:** Quorum reached! Initiate commit phase

8. **Primary → All Replicas:** Send gRPC `RecvCommit(opID)` (unary RPC)
   - Sent in parallel to all replicas
   - Continues even if some fail (quorum already satisfied)

9. **Replicas:** Look up StagedChunk by opID
   - Uses ChunkStagingTrackingService singleton

10. **Replicas:** Call `Commit()` - write to disk with fsync
    - Open/create chunk file: `{storageDir}/{chunkHandle}`
    - Seek to offset
    - Write buffer contents
    - **fsync() for durability**
    - Update status to COMMIT

11. **Replicas → Primary:** Return success/failure

12. **Primary:** Commit locally to disk
    - Same process: write + fsync

13. **Primary → Client:** Send success (byte=1) or failure (byte=0)

## Read Path
1. **Client → Master:** Get file plan (ordered chunks + lengths).
2. **Client → Chunkservers:** Fetch each chunk `[0..length)` with checksum verification.
3. Chunks are concatenated in order to reconstruct the file.

## Fault Tolerance
- Replication factor of 3; system tolerates 1 replica failure.
- Checksums detect corruption on reads and writes.
- Master re-replicates missing or corrupted chunks in the background.
- Replicas truncate to last valid checksum on crash recovery.

## Supported Operations
- **Create/Delete File**
- **Append to File**
- **Read File (whole file or by chunks)**

## Limitations (MVP)
- Append-only (no overwrite or random writes).
- Single master (no HA yet).
- Quorum=2/3 fixed (no dynamic policies).
- Best-effort at-least-once semantics (clients deduplicate via checksums/IDs).
