# Consumer Groups & Rebalancing

Kafka Lite supports distributed Consumer Groups to allow parallel processing of a topic.

## Partition Assignment
When consumers `subscribe` to a group, the `ConsumerGroupManager` assigns them exclusive ownership of specific partitions using a Round Robin algorithm.
For example, if a topic has 4 partitions and there are 2 consumers, each consumer is assigned 2 partitions.

## Heartbeats & Fault Tolerance
Consumers must send a `{"action": "heartbeat"}` every 5 seconds.
The `HeartbeatManager` runs a background thread. If it detects a consumer has not sent a heartbeat in `consumerTimeoutSeconds` (default 15s), the consumer is evicted from the group, and a rebalance is triggered automatically. The dead consumer's partitions are instantly redistributed to the remaining healthy consumers.
