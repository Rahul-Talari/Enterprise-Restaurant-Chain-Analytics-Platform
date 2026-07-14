# Apache Kafka & Azure Event Hubs - Notes

## Apache Kafka Overview
Apache Kafka is an open-source, distributed event streaming platform designed for high-throughput, real-time data pipelines. It enables applications to write and read event streams in a durable, ordered, and scalable way.

### Traditional Architecture
- Tightly coupled, synchronous communication
- Producers wait for consumers
- Adding new consumers often requires producer changes

### Kafka Architecture
- Loosely coupled, asynchronous system
- Producers publish messages to topics
- Multiple consumer groups can independently consume the same data
- New consumers can be added without modifying producers

<p align="center">
  <img src="../diagrams/Kafka_Architecture.png" width="100%">
</p>

### 1. Core Infrastructure (Cluster & Storage)

**Kafka Cluster**
- A group of brokers working together for scalability and fault tolerance
- Managed via:
  - KRaft (modern Kafka)
  - ZooKeeper (legacy systems)
- Handles metadata, controller quorum, and leader election

**Broker**
- Kafka server that stores partition logs
- Handles producer and consumer requests
- Supports replication for fault tolerance
- Leader handles reads/writes, followers replicate data

---

### 2. Data Organization

**Topic**
- Logical stream of data (like a table)

**Partition**
- Physical, ordered log of messages
- Each message has a unique offset
- Ensures ordering within a partition
- Enables parallel processing

---

### 3. Kafka Message Structure
- Key
- Value
- Headers
- Timestamp

Retention policies:
- Time-based (log.retention.hours)
- Size-based (log.retention.bytes)

---

### 4. Producers (Write Path)
- Publish messages to topics
- Fire-and-forget model (async)
- Uses bootstrap broker to connect
- Partitioning via key or round-robin
- Supports batching, compression, retries, acknowledgments

---

### 5. Consumers (Read Path)
- Subscribe to topics
- Read messages from partitions
- Track progress using offsets
- Can replay data by resetting offsets

**Consumer Groups**
- Across groups: independent consumption
- Within group: one partition per consumer
- Max parallelism = number of partitions

---

## Kafka Benefits
- High throughput (sequential log-based storage)
- Fault tolerance (replication)
- Scalability (brokers + partitions)
- Durability (persistent logs)

---

## Azure Event Hubs Overview
Azure Event Hubs is a big data event streaming platform for ingesting millions of events per second with high reliability and low latency.

### Core Concepts

#### Infrastructure Layer
- Namespace (Cluster)
- Broker services
- Managed by Azure (KRaft-like internal management)

#### Data Storage Layer
- Event Hub (Topic equivalent)
- Partitions
- Retention policies
- Replication (managed by Azure)

#### Ingestion & Consumption Layer
- Producers send events
- Consumers read events
- Consumer groups enable parallel processing

#### **Security Layer** : Send policies, Listen policies, Manage policies

---

## Comparison Summary

| Feature | Kafka | Azure Event Hubs |
|--------|------|------------------|
| Type | Open-source platform | Managed cloud service |
| Scaling | Manual (brokers/partitions) | Automatic |
| Management | Self-managed or cloud | Fully managed |
| Storage | Distributed log | Managed event stream |
| Consumers | Consumer groups | Consumer groups |
