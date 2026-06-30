# Storage System

Kafka Lite utilizes a high-performance persistent append-only storage model.

## Directory Layout
```text
data/
└── topic_name/
      ├── 0.log (Partition 0)
      ├── 0.idx
      ├── 1.log (Partition 1)
      └── 1.idx
```

## Logs and Indexes
- **`.log` files**: Raw binary appending of messages. Each message is prefixed with its size.
- **`.idx` files**: In-memory mapping of `offset` -> `byte position`. Enables O(1) random access when consumers request specific offsets.

## Retention Policies
The `RetentionManager` thread scans the `data/` directory every 60 seconds.
- **Time-based**: Logs older than `retentionHours` are cleared.
- **Size-based**: If a partition directory exceeds `maxLogSizeMB`, old logs are truncated to free disk space.
