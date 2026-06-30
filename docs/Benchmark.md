# Benchmarks

The `benchmark/` folder contains Python scripts to stress test the broker.

## Single Producer (No Batching, Fire & Forget)
With `acks=0`, the broker can ingest single messages at extremely high rates.
```bash
python benchmark/benchmark_producer.py
```
**Results:** ~50,000+ msg/sec depending on hardware.

## Batch Publishing (Acks=1)
Using `publish_batch` reduces network overhead significantly.
```bash
python benchmark/benchmark_batch.py
```
**Results:** ~100,000+ msg/sec depending on hardware.

## Profiling
- CPU Usage: The multi-threaded TCP server efficiently utilizes all cores defined by `threadPoolSize` in `config.json`.
- Memory Usage: Mostly bounded by the `.idx` files loaded into memory.
- Disk Usage: Disk writes are append-only. The OS page cache handles batching to physical disk, resulting in near-memory speeds.
