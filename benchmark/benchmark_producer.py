import socket
import json
import time
import threading

def run_producer(num_messages):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 9092))
    
    for i in range(num_messages):
        req = {
            "action": "publish",
            "topic": "benchmark",
            "value": f"msg_{i}",
            "acks": 0 # fire and forget for max throughput
        }
        s.sendall((json.dumps(req) + "\n").encode())
    s.close()

if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 9092))
    s.sendall((json.dumps({"action": "create_topic", "topic": "benchmark", "partitions": 4}) + "\n").encode())
    s.close()
    
    print("Starting 10 Producers, 10000 messages each...")
    threads = []
    start_time = time.time()
    
    for _ in range(10):
        t = threading.Thread(target=run_producer, args=(10000,))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    end_time = time.time()
    duration = end_time - start_time
    tps = 100000 / duration
    print(f"Total time: {duration:.2f}s")
    print(f"Throughput: {tps:.2f} msg/sec")
