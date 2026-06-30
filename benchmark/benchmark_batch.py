import socket
import json
import time

if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 9092))
    s.sendall((json.dumps({"action": "create_topic", "topic": "benchmark_batch", "partitions": 4}) + "\n").encode())
    
    print("Starting Batch Publisher (100,000 messages in batches of 1000)...")
    start_time = time.time()
    
    for _ in range(100):
        messages = [{"topic": "benchmark_batch", "value": "x"} for _ in range(1000)]
        req = {
            "action": "publish_batch",
            "messages": messages,
            "acks": 1
        }
        s.sendall((json.dumps(req) + "\n").encode())
        resp = s.recv(8192) # wait for ack
        
    end_time = time.time()
    s.close()
    
    duration = end_time - start_time
    tps = 100000 / duration
    print(f"Total time: {duration:.2f}s")
    print(f"Throughput: {tps:.2f} msg/sec")
