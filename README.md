# hermes: the messenger of gods

A personal implementation of a **high-performance append-only log / streaming platform in pure Rust**, inspired by Kafka.

The goal is to understand how log storage, batching, indexing, segmentation, and eventually distributed streaming systems work under the hood.


## Architecture
<img width="2113" height="744" alt="image" src="https://github.com/user-attachments/assets/af47e5bc-2fec-4b26-8db8-561031261134" />

---

## Test
<img width="1536" height="260" alt="image" src="https://github.com/user-attachments/assets/afec36fc-384d-4579-969d-b82c771b7cdc" />

---

## Storage Format

Each log record is stored as:

```text
[offset][length][serialized log]
```

The sparse index stores:

```text
[offset][position]
```

The index is used to find the nearest known position, after which the log file is scanned until the requested offset is found.

---

## Core Features

* **Single Writer** — keeps disk writes sequential and avoids multiple writers competing for the log.
* **Batching** — groups 100 logs before writing them to disk.
* **Append-only Storage** — logs are sequentially appended rather than modified in place.
* **Sparse Indexing** — stores selected offsets and their file positions for faster lookups.
* **Segments** — rolls over to a new log file when the current segment reaches its size limit.
* **Offset-based Consumption** — consumers request logs using their offset.

---

## Tech Stack

* Rust
* Tokio
* WebSockets
* Serde
* bincode
* Tokio `mpsc`

---

## Roadmap

```text
[✅] WebSocket producer
[✅] Producer queue
[✅] Single writer
[✅] Batch writing
[✅] Sparse indexing
[✅] Consumer
[✅] Segments
[ ] Crash recovery
[ ] Log retention
[ ] WebSocket consumer
[ ] Producer acknowledgements
[ ] Topics / partitions
[ ] Consumer groups
[ ] Replication
[ ] Leader election
[ ] Broker coordination
```

---

## Long-Term Goal

Evolve hermes from a single-node append-only log into a **distributed streaming system** with topics, partitions, replication, consumer groups, and fault tolerance.
