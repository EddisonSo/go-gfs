# Append-Only Distributed File System (GFS-style)

## Overview
This project implements a simplified Google File System (GFS)-like storage system optimized for **append-only workloads** such as logs.  
It provides a **master** for metadata management, **chunkservers** for data storage, and supports fault-tolerant appends and reads with replication.

## Core Concepts
- **Chunks:** Files are split into fixed-size chunks (default 64MB). Each chunk is replicated (RF=3).
- **Master:** Stores metadata (file → chunk mapping, replica sets, chunk lengths). Handles namespace, placement, heartbeats, and repair.
- **Chunkservers:** Store raw chunks, checksums, and handle read/write requests. They are stateless regarding file layout.
- **Append-only writes:** Data is appended atomically to the tail chunk. Once full, a new chunk is created.

## Write Path (Append)
1. **Client → Master:** Request chunk locations for file append.
2. **Client → Replicas:** Send data (staged as `READY`).
3. **Sequencer (primary or gateway):** Assigns order, offset, and sequence number.
4. **Commit:** After quorum (`2/3`) replicas confirm `READY`, sequencer issues `COMMIT`.
5. **Ack:** Client receives final offset.

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
