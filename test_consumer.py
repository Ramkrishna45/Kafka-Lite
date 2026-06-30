import socket
import json

def test_consume():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(('127.0.0.1', 9092))
        
        req = {
            "action": "consume",
            "topic": "orders",
            "group": "group_a",
            "partition": 0, # Depending on hash of key, we may need to check other partitions
            "max": 10
        }
        
        s.sendall((json.dumps(req) + "\n").encode())
        resp = s.recv(4096)
        print("Consumer Response:", resp.decode().strip())
    except Exception as e:
        print("Failed to connect:", e)
    finally:
        s.close()

if __name__ == "__main__":
    test_consume()
