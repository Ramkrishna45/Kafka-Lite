# Kafka Lite Architecture

Kafka Lite is a high-throughput, persistent, multi-threaded message queue inspired by Apache Kafka.

## Core Components
1. **Broker**: The central hub that instantiates and ties all subsystems together.
2. **TcpServer**: A Winsock/POSIX based server that handles inbound connections and parses JSON payloads using `nlohmann/json`. It routes requests to the Broker.
3. **TopicManager**: Maintains the registry of `Topic` objects. Each topic contains multiple `Partition` objects.
4. **Storage Engine**: Each `Partition` holds a `LogSegment`, which uses append-only flat files (`.log`) and in-memory indexes (`.idx`) for O(1) read/write performance.
5. **ConsumerGroupManager**: Handles dynamic assignments of partitions to consumers using a Round Robin algorithm.
6. **HeartbeatManager**: Background thread that detects consumer failures (15s timeout) and triggers rebalancing.
7. **RetentionManager**: Background thread that periodically deletes log files exceeding the maximum age (`retentionHours`) or maximum size (`maxLogSizeMB`).
8. **MetricsManager**: Centrally tracks global throughput (Messages Published/Consumed).
9. **Logger & ConfigManager**: Structured console logging and dynamic JSON configuration.

## Concurrency Model
The system uses a lock-free thread pool to handle concurrent TCP connections. Internal states are protected using fine-grained `std::shared_mutex` (Readers-Writer locks), allowing parallel reads while ensuring safe writes. Partitions can be appended to completely concurrently by different threads.
