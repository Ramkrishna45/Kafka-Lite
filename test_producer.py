import socket
import json

def test_publish():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(('127.0.0.1', 9092))
        
        req = {
            "action": "publish",
            "topic": "orders",
            "key": "order_123",
            "value": "iPhone 15 Pro Max"
        }
        
        s.sendall((json.dumps(req) + "\n").encode())
        resp = s.recv(4096)
        print("Producer Response:", resp.decode().strip())
    except Exception as e:
        print("Failed to connect:", e)
    finally:
        s.close()

if __name__ == "__main__":
    test_publish()
