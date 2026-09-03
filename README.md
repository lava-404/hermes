# Mercury 

A personal implementation of a **high-performance distributed append-only log / streaming platform in pure Rust**, inspired by Kafka.

This README is mainly a progress tracker and reminder of what has been implemented and what comes next.

---

## September 2, 2026

### Completed

* ✅ Implemented **batch writing**
* ✅ Implemented **producer-side queue** using `mpsc`
* ⏳ Consumer-side reading is still remaining

---

## Architecture

Current intended data path:

```text
WebSocket connections
        ↓
     produce()
        ↓
       Queue
        ↓
   Single Writer
        ↓
   Batch 100 logs
        ↓
      Segment
        ↓
   Sequential File Write
        ↓
       Disk
```

The goal is to keep the data path simple and fast:

**Producers → Queue → Single Writer → Batched Sequential I/O → Disk**

---

## Next To Do

### 1. Consumer

📮 Implement consumer-side reading of logs.

Need to implement:

* Read logs from the log file
* Use the stored offset/index information
* Properly separate individual records using `[offset][length][payload]`
* Deserialize the payload
* Return the requested log to the consumer

---

### 2. Segments & Batch Management

📮 Implement segments and basic batch management.

Need to work on:

* Segment creation
* Segment rolling
* Basic retention
* Managing batches within segments
* Coordinator responsible for overseeing batching/segment management

---

### 3. WebSocket Responses

📮 Use **oneshot** to send responses/status messages back to producers through the WebSocket.

Potential flow:

```text
Producer
   ↓
WebSocket
   ↓
Queue
   ↓
Single Writer
   ↓
Disk
   ↓
Write Result
   ↓
oneshot
   ↓
Producer
```

---

## Current Storage Format

Each log record is stored as:

```text
[offset][length][payload]
```

The index stores:

```text
[offset][position]
```

The index will eventually store entries only at the chosen interval rather than for every log.

---

## Core Ideas

* Binary serialization using `bincode`
* `mpsc` queue for producer → writer communication
* Single writer for sequential disk writes
* Batch multiple logs into one contiguous byte buffer
* Sequential file I/O for performance
* Sparse indexing for faster log lookup
* Segments for log storage and retention
* Eventually: topics/partitions, leadership, replication, and coordination

---

## Progress

```text
[✅] WebSocket producer
[✅] Producer-side queue
[✅] Single writer
[✅] Batch writing
[ ] Consumer
[ ] Log deserialization/separation
[ ] Segments
[ ] Batch management
[ ] Retention
[ ] Segment rolling
[ ] WebSocket responses
[ ] Topics / partitions
[ ] Coordinator
[ ] Leadership
[ ] Replication
```
