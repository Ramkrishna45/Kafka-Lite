import socket
import json
import time

def send_request(req):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(('127.0.0.1', 9092))
        s.sendall((json.dumps(req) + "\n").encode())
        resp = s.recv(8192).decode().strip()
        return json.loads(resp) if resp else None
    except Exception as e:
        print(f"Error connecting/receiving: {e}")
        return None
    finally:
        s.close()

def run_tests():
    print("1. Creating Topic 'metrics' with 4 partitions...")
    send_request({"action": "create_topic", "topic": "metrics", "partitions": 4})

    print("\n2. Subscribing Consumer1 and Consumer2 to Group 'dashboards'...")
    send_request({"action": "subscribe", "group": "dashboards", "topic": "metrics", "consumerId": "c1"})
    send_request({"action": "subscribe", "group": "dashboards", "topic": "metrics", "consumerId": "c2"})

    print("\n3. Checking Group Assignments...")
    groups_info = send_request({"action": "groups"})
    print(json.dumps(groups_info, indent=2))
    
    print("\n4. Publishing 10 messages (Round Robin)...")
    for i in range(10):
        send_request({"action": "publish", "topic": "metrics", "value": f"msg_{i}"})
        
    print("\n5. Consumer1 consuming (from its assigned partitions)...")
    c1_msgs = send_request({"action": "consume", "group": "dashboards", "topic": "metrics", "consumerId": "c1", "max": 10})
    if c1_msgs and "messages" in c1_msgs:
        print(f"C1 Received {len(c1_msgs['messages'])} messages.")
    
    print("\n6. Consumer2 consuming (from its assigned partitions)...")
    c2_msgs = send_request({"action": "consume", "group": "dashboards", "topic": "metrics", "consumerId": "c2", "max": 10})
    if c2_msgs and "messages" in c2_msgs:
        print(f"C2 Received {len(c2_msgs['messages'])} messages.")
    
    print("\n7. Simulating Consumer1 crash...")
    print("Heartbeating C2 only. C1 will time out in ~15-20s. Waiting...")
    for _ in range(4):
        send_request({"action": "heartbeat", "consumerId": "c2"})
        print("C2 Heartbeat sent, waiting 5s...")
        time.sleep(5)
        
    print("\n8. Checking Assignments (C1 should be evicted, C2 owns all partitions)...")
    groups_info = send_request({"action": "groups"})
    print(json.dumps(groups_info, indent=2))
    
    print("\n9. Checking Metrics for Consumer Lag...")
    metrics_info = send_request({"action": "metrics"})
    print(json.dumps(metrics_info, indent=2))

if __name__ == "__main__":
    run_tests()
