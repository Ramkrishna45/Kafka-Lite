import socket
import json

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
    print("1. Creating Topic 'events' with 4 partitions...")
    resp = send_request({
        "action": "create_topic",
        "topic": "events",
        "partitions": 4
    })
    print("Response:", resp)

    print("\n2. Testing Key-based Hashing (same key -> same partition)...")
    partitions_used = set()
    for _ in range(10):
        resp = send_request({
            "action": "publish",
            "topic": "events",
            "key": "user_id_999",
            "value": "login_event"
        })
        if resp and "partition" in resp:
            partitions_used.add(resp["partition"])
    
    print(f"Partitions used for key 'user_id_999': {partitions_used}")
    if len(partitions_used) == 1:
        print("-> SUCCESS: Consistent hashing verified.")
    else:
        print("-> FAILED: Multiple partitions used for the same key.")
    
    print("\n3. Testing Round-Robin (no key -> distributed across partitions)...")
    rr_partitions = []
    for _ in range(12):
        resp = send_request({
            "action": "publish",
            "topic": "events",
            "value": "anonymous_click"
        })
        if resp and "partition" in resp:
            rr_partitions.append(resp["partition"])
    
    print(f"Partitions used for round robin: {rr_partitions}")
    unique_rr = set(rr_partitions)
    if len(unique_rr) > 1:
        print("-> SUCCESS: Round Robin distributed messages across partitions.")
    else:
        print("-> FAILED: Round Robin did not distribute messages.")

    print("\n4. Testing Metrics API...")
    metrics = send_request({
        "action": "metrics"
    })
    print(json.dumps(metrics, indent=2))

if __name__ == "__main__":
    run_tests()
