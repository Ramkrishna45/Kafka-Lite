import socket
import json
import time

if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 9092))
    
    # Create topic
    s.sendall((json.dumps({"action": "create_topic", "topic": "continuous_test", "partitions": 4}) + "\n").encode())
    
    print("Starting Continuous Publisher for 10 seconds...")
    start_time = time.time()
    
    batches_sent = 0
    while time.time() - start_time < 10:
        # Reduced batch size to 50 so it fits safely within the broker's 4096 byte TCP buffer!
        messages = [{"topic": "continuous_test", "value": "x"} for _ in range(50)]
        req = {
            "action": "publish_batch",
            "messages": messages,
            "acks": 0 # Fire and forget for max throughput
        }
        s.sendall((json.dumps(req) + "\n").encode())
        batches_sent += 1
        
        # Sleep a tiny bit to not completely overload the OS socket buffer
        time.sleep(0.005)
        
    end_time = time.time()
    s.close()
    
    total_messages = batches_sent * 50
    duration = end_time - start_time
    tps = total_messages / duration
    print(f"Total time: {duration:.2f}s")
    print(f"Total messages: {total_messages}")
    print(f"Throughput: {tps:.2f} msg/sec")
