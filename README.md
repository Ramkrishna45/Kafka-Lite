# Kafka Lite v1.0

Kafka Lite is a high-throughput, persistent, multi-threaded message broker inspired by Apache Kafka. It implements core distributed messaging concepts including topic partitioning, persistent append-only logs, consumer groups, automatic rebalancing, and consumer lag tracking—without the overhead of heavy distributed systems like Zookeeper.

## 🌟 Key Features

- **Topics & Partitions**: Messages are routed using Key-based hashing or Round-Robin algorithms to support massive concurrency across multiple partitions.
- **Consumer Groups**: Distributed processing with automatic, dynamic partition assignment to group members.
- **Fault Tolerance**: Background heartbeat mechanisms detect consumer crashes and trigger automatic partition rebalancing.
- **Persistent Storage**: O(1) append-only disk storage leveraging memory-mapped-like index lookups for blazing fast retrieval.
- **Retention Policies**: Configurable background thread for time-based and size-based automatic log cleanup.
- **Observability**: Real-time APIs to track TPS (Throughput), consumer lag, and broker health.
- **High Performance**: Lock-free atomic counters, Readers-Writer locks, and `publish_batch` support for 500k+ messages per second.
- **Web Dashboard**: Included zero-dependency Python dashboard for real-time monitoring of metrics.

---

## 🛠️ Building and Running

### Prerequisites
- C++17 Compiler (Visual Studio MSVC recommended for Windows)
- CMake 3.14+
- Python 3 (For running benchmarks, tests, and the dashboard)

### Build the Broker
```bash
mkdir build
cd build
cmake ..
cmake --build .
```

### Start the Broker
The broker will automatically read the `config.json` file in the root directory.
```bash
./build/Debug/kafka_lite.exe
```

### Start the Dashboard
In a separate terminal, start the Python dashboard server:
```bash
python dashboard/server.py
```
Open `http://localhost:8080` in your web browser.

---

## 📐 Architecture Diagrams

The following diagrams accurately represent the specific C++ implementation of this broker.

### 1. Overall Component Architecture
```mermaid
graph TD
    Client[Client / Producer / Consumer] -->|TCP / JSON| TcpServer[TcpServer]
    
    subgraph Kafka Lite Broker
        TcpServer -->|Parsed Request| Broker[Broker]
        
        Broker --> TopicManager[TopicManager]
        Broker --> ConsumerGroupManager[ConsumerGroupManager]
        Broker --> OffsetManager[OffsetManager]
        Broker --> HeartbeatManager[HeartbeatManager]
        Broker --> MetricsManager[MetricsManager]
        Broker --> RetentionManager[RetentionManager]
        
        TopicManager --> Topic1[Topic]
        TopicManager --> Topic2[Topic]
        
        Topic1 --> Part0[Partition 0]
        Topic1 --> Part1[Partition 1]
        
        Part0 --> LogSeg0[LogSegment]
        Part1 --> LogSeg1[LogSegment]
        
        LogSeg0 --> Disk[(Disk / data/)]
        RetentionManager -.->|Truncates old files| Disk
    end
    
    ConfigManager[ConfigManager] -.->|Loads| config.json
    Logger[Logger] -.->|Console Output| STDOUT
```

### 2. Class Relationship Diagram
```mermaid
classDiagram
    class Broker {
        -TopicManager topic_manager_
        -ConsumerGroupManager group_manager_
        -OffsetManager offset_manager_
        -HeartbeatManager heartbeat_manager_
        -MetricsManager metrics_manager_
        -RetentionManager retention_manager_
        +publish(topic, msg)
        +consume(group, topic, consumer_id, max)
        +subscribe(group, topic, consumer_id)
        +heartbeat(consumer_id)
    }

    class TcpServer {
        -int port_
        -Broker* broker_
        -ThreadPool thread_pool_
        +start()
        +handle_client(socket)
    }

    class TopicManager {
        -unordered_map~string, Topic~ topics_
        -shared_mutex mutex_
        +create_topic(name, partitions)
        +get_topic(name)
    }

    class Topic {
        -string name_
        -vector~Partition~ partitions_
        +get_partition(key)
        +get_partition_by_id(id)
    }

    class Partition {
        -int id_
        -LogSegment log_
        +append(msg)
        +read(offset, max)
    }

    class LogSegment {
        -fstream file_
        -mutex mutex_
        +append(msg)
        +read(offset, max)
    }

    class ConsumerGroupManager {
        -unordered_map~string, ConsumerGroup~ groups_
        -shared_mutex mutex_
        +subscribe(group, topic, consumer)
        +rebalance(group, topic)
    }

    TcpServer --> Broker
    Broker *-- TopicManager
    Broker *-- ConsumerGroupManager
    Broker *-- OffsetManager
    Broker *-- HeartbeatManager
    Broker *-- MetricsManager
    Broker *-- RetentionManager
    TopicManager *-- Topic
    Topic *-- Partition
    Partition *-- LogSegment
```

### 3. Publish Flow Sequence Diagram
```mermaid
sequenceDiagram
    participant P as Producer
    participant S as TcpServer
    participant B as Broker
    participant T as TopicManager
    participant Ptn as Partition
    participant L as LogSegment

    P->>S: {"action": "publish", "topic": "orders", "key": "k1", "value": "v1", "acks": 1}
    S->>B: publish("orders", msg)
    B->>T: get_topic("orders")
    T-->>B: Topic pointer
    B->>Topic: get_partition("k1") (Key hashing)
    Topic-->>B: Partition 1
    B->>Ptn: append(msg)
    Ptn->>L: append(msg)
    L->>L: Lock std::mutex
    L->>Disk: Write Binary Data
    L-->>Ptn: success, offset
    Ptn-->>B: result
    B->>MetricsManager: increment_published()
    B-->>S: PublishResult
    S->>P: {"status": "success", "partition": 1, "offset": 42}
```

### 4. Consume Flow Sequence Diagram
```mermaid
sequenceDiagram
    participant C as Consumer
    participant S as TcpServer
    participant B as Broker
    participant GM as ConsumerGroupManager
    participant OM as OffsetManager
    participant T as TopicManager
    participant Ptn as Partition

    C->>S: {"action": "consume", "group": "analytics", "topic": "orders", "consumerId": "c1", "max": 10}
    S->>B: consume("analytics", "orders", "c1", 10)
    
    B->>GM: check_assignment("analytics", "c1")
    GM-->>B: [Partition 0, Partition 1]
    
    B->>OM: get_offset("analytics", "orders", 0)
    OM-->>B: offset = 42
    
    B->>T: get_topic("orders")
    T-->>B: Topic
    
    B->>Topic: get_partition(0)
    Topic-->>B: Partition 0
    
    B->>Ptn: read(42, 10)
    Ptn->>Disk: Read Binary Data
    Ptn-->>B: [msg1, msg2, ...]
    
    B->>OM: commit_offset("analytics", "orders", 0, 42 + messages.size())
    B->>MetricsManager: add_consumed(messages.size())
    
    B-->>S: vector<Message>
    S->>C: {"status": "success", "messages": [...]}
```

### 5. Consumer Group Rebalancing Sequence Diagram
```mermaid
sequenceDiagram
    participant HM as HeartbeatManager (Background Thread)
    participant B as Broker
    participant GM as ConsumerGroupManager

    Note over HM, GM: Runs continuously every second
    
    HM->>HM: Check last heartbeat timestamps
    alt Consumer c1 timeout (>15s)
        HM->>B: evict_consumer("analytics", "c1")
        B->>GM: remove_consumer("analytics", "c1")
        GM->>GM: Lock std::unique_lock
        GM->>GM: Remove "c1" from group
        GM->>GM: Trigger rebalance("analytics")
        Note over GM: Round-Robin Algorithm:
        Note over GM: P0->c2, P1->c3, P2->c2, P3->c3
        GM-->>B: success
    end
```

### 6. Startup & Recovery Flow
```mermaid
flowchart TD
    Start["main.cpp"] --> LoadConfig["ConfigManager::load()"]
    LoadConfig --> ReadJSON["Parse config.json"]
    ReadJSON --> InitBroker["Instantiate Broker"]
    
    InitBroker --> InitDirs["Check / create data/ folder"]
    InitBroker --> InitTopicMgr["Instantiate TopicManager"]
    InitBroker --> InitThreads["Start Background Threads"]
    
    InitThreads --> HeartbeatTh["HeartbeatManager Thread"]
    InitThreads --> RetentionTh["RetentionManager Thread"]
    
    InitBroker --> StartTCP["Instantiate & Start TcpServer"]
    StartTCP --> Listen["Socket bind() & listen()"]
    Listen --> Accept["accept() loop in main thread"]
```

### 7. Thread Architecture
```mermaid
graph TD
    MainThread["Main Thread"] -->|Accepts Connections| TcpServer
    
    subgraph TCP_ThreadPool ["TCP Thread Pool (e.g. 8 threads)"]
        Worker1["Worker Thread 1"] -->|Parses JSON, calls Broker API| ClientSocket1
        Worker2["Worker Thread 2"] -->|Parses JSON, calls Broker API| ClientSocket2
        Worker3["Worker Thread 3"]
        Worker4["Worker Thread 4"]
    end
    
    TcpServer -.->|Enqueues Client Socket| TCP_Queue
    TCP_Queue -.-> Worker1
    TCP_Queue -.-> Worker2
    
    subgraph Background_Threads ["Background Threads"]
        Heartbeat["HeartbeatManager Thread"] -->|Check timeouts every 1s| Broker
        Retention["RetentionManager Thread"] -->|Check disk usage every 60s| Disk
    end
    
    Worker1 --> |std::shared_lock| SharedState["Shared Metadata"]
    Worker2 --> |std::unique_lock| SharedState
```

### 8. Storage Architecture
```mermaid
graph LR
    Topic["Topic"] --> Part0["Partition 0"]
    Topic --> Part1["Partition 1"]
    
    subgraph Part0_Dir ["Partition 0 Directory"]
        Log0["partition0.log"]
        Idx0["partition0.idx"]
    end
    
    Part0 --> Log0
    Part0 --> Idx0
    
    LogNote["Append-only flat file.<br/>Binary serialization.<br/>Message = [Size][Timestamp][KeyLen][Key][ValLen][Val]"]
    IdxNote["In-memory hash map.<br/>Maps offset to byte position.<br/>Rebuilt on startup if missing."]
    
    Log0 -.-> LogNote
    Idx0 -.-> IdxNote
```

### 9. Network Architecture
```mermaid
sequenceDiagram
    participant Client
    participant OS as OS Socket
    participant TcpServer as TcpServer Main Thread
    participant ThreadPool as Worker Thread
    participant Broker as Broker Engine

    Client->>OS: TCP Connect (Port 9092)
    OS-->>TcpServer: accept() returns Client Socket
    TcpServer->>ThreadPool: enqueue(handle_client)
    
    loop While Connected
        Client->>OS: Send raw bytes ("{"action": "publish"...}\n")
        OS-->>ThreadPool: recv() buffer
        ThreadPool->>ThreadPool: Parse nlohmann::json
        ThreadPool->>Broker: Execute action
        Broker-->>ThreadPool: Return response struct
        ThreadPool->>ThreadPool: Serialize to JSON string
        ThreadPool->>OS: send() bytes ("{"status": "success"}\n")
        OS-->>Client: TCP Response
    end
```

---

## 📈 Benchmarks (Real World Data)

These benchmarks were generated on a standard development machine using the automated `benchmark/run_all_benchmarks.py` script.

| Test | Result |
|------|--------|
| **Single Producer Throughput** | 527,804 msg/sec |
| **10 Producers Throughput** (Disk Bound) | 326,298 msg/sec |
| **Batch Size 1000 Latency** (acks=1) | 26.45 ms |
| **Recovery Time** (1 Million Msgs) | 0.51 sec |

### Performance Graphs

**Throughput vs Producers**

![Throughput vs Producers](docs/images/throughput_vs_producers.png)

**Latency vs Batch Size**

![Latency vs Batch Size](docs/images/latency_vs_batch_size.png)

**Recovery Time vs Messages**

![Recovery Time](docs/images/recovery_time.png)
