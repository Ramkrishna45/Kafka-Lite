import http.server
import socketserver
import json
import socket
import os
from urllib.parse import urlparse

PORT = 8080
BROKER_IP = '127.0.0.1'
BROKER_PORT = 9092

def send_tcp_request(req):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect((BROKER_IP, BROKER_PORT))
        s.sendall((json.dumps(req) + "\n").encode())
        resp = s.recv(65536).decode().strip()
        s.close()
        return json.loads(resp) if resp else {}
    except Exception as e:
        print(f"TCP Error: {e}")
        return {"error": str(e)}

def get_disk_usage(directory):
    total_size = 0
    if os.path.exists(directory):
        for dirpath, dirnames, filenames in os.walk(directory):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    return total_size

class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # We assume the server is run from the root of the project (d:/WEB-D/New folder (2))
        super().__init__(*args, directory="dashboard/public", **kwargs)

    def do_GET(self):
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/api/metrics':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            # Fetch metrics and groups
            metrics = send_tcp_request({"action": "metrics"})
            groups = send_tcp_request({"action": "groups"})
            
            # Combine them
            response = {
                "metrics": metrics,
                "groups": groups
            }
            self.wfile.write(json.dumps(response).encode())
            
        elif parsed_path.path == '/api/disk':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            # Path to data folder - check possible locations depending on where broker is running
            possible_paths = [
                os.path.join(os.getcwd(), 'data'),
                os.path.join(os.getcwd(), 'build', 'data'),
                os.path.join(os.getcwd(), 'build', 'Debug', 'data')
            ]
            
            size_bytes = 0
            for p in possible_paths:
                if os.path.exists(p):
                    size_bytes += get_disk_usage(p)
                    
            response = {
                "diskUsageBytes": size_bytes
            }
            self.wfile.write(json.dumps(response).encode())
            
        else:
            # Serve static files
            super().do_GET()

if __name__ == '__main__':
    with socketserver.TCPServer(("", PORT), DashboardHandler) as httpd:
        print(f"Dashboard running on http://localhost:{PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down dashboard...")
