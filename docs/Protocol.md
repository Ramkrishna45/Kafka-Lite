# Kafka Lite Protocol

Communication with the broker happens over a raw TCP socket using newline-delimited JSON payloads.

## Publishing
**Request:**
```json
{
  "action": "publish",
  "topic": "orders",
  "key": "user123",
  "value": "iPhone",
  "acks": 1
}
```
**Response:**
```json
{
  "status": "success",
  "partition": 0,
  "offset": 45
}
```

## Batch Publishing
**Request:**
```json
{
  "action": "publish_batch",
  "acks": 1,
  "messages": [
    {"topic": "orders", "value": "A"},
    {"topic": "orders", "value": "B"}
  ]
}
```
**Response:**
```json
{"status": "success", "published": 2}
```

## Consuming
**Request:**
```json
{
  "action": "consume",
  "group": "analytics",
  "topic": "orders",
  "consumerId": "c1",
  "max": 10
}
```
**Response:**
```json
{
  "status": "success",
  "messages": [
    {"partition": 0, "offset": 45, "value": "iPhone", "timestamp": 16900000}
  ]
}
```

## Observability
- `{"action": "metrics"}`
- `{"action": "health"}`
- `{"action": "topics"}`
- `{"action": "groups"}`
