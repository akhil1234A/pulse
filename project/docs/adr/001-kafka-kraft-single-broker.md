# ADR 001: Apache Kafka KRaft Mode with Single-Broker Topology for Local Deployment

## Status
Accepted

## Context
PulseStream requires a durable, distributed message broker capable of handling tens of thousands of streaming events per second with high partition throughput and low publish latency. Traditionally, Apache Kafka required Apache ZooKeeper for cluster metadata management and controller election. In Apache Kafka 3.x+, KRaft (Kafka Raft Metadata mode) replaces ZooKeeper by maintaining metadata in internal Kafka quorum partitions (`@metadata`).

For local development, testing, and portfolio demonstrations, we needed to decide between:
1. Multi-container Kafka + ZooKeeper setup
2. Multi-broker KRaft cluster (3+ brokers)
3. Single-broker KRaft deployment

## Decision
We decided to deploy **Apache Kafka 3.7+ in KRaft mode with a single broker (`broker-1`)** for local development, CI/CD, and demonstration environments.

Configuration highlights:
- `KAFKA_PROCESS_ROLES: broker,controller`
- `KAFKA_NODE_ID: 1`
- `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093`
- `default.replication.factor: 1`
- `offsets.topic.replication.factor: 1`
- `transaction.state.log.replication.factor: 1`

## Consequences

### Positive
- **Resource Footprint**: Eliminates the 500MB+ memory and CPU overhead of running a ZooKeeper container. Total memory consumption drops from ~1.5GB to under 600MB.
- **Fast Startup**: A single KRaft container initializes in under 5 seconds, compared to 20-30 seconds for ZooKeeper + Kafka handshakes.
- **Future-Proof**: ZooKeeper has been deprecated in Kafka 3.5+ and removed in Kafka 4.0. Using KRaft aligns with current modern distributed systems standards.
- **Clean Developer Experience**: One fewer container in `docker-compose.yml`, simplified health checks, and deterministic port binding.

### Negative / Trade-offs
- **Zero Fault Tolerance to Node Failure**: A single broker cannot tolerate node crashes without service disruption (mitigated by Docker container restart policies in local dev).
- **Partition Replication Factor of 1**: Data is not replicated across independent physical disks.

### Production Path
In a production deployment on AWS or GCP, the cluster will scale to 3 dedicated KRaft controllers and 3 or more brokers with `replication.factor=3` and `min.insync.replicas=2` without requiring changes to producer or consumer application code.
