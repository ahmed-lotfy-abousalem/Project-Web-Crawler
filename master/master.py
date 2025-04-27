import socket
import threading
import queue
import json

task_queue = queue.Queue()
crawled_results = []

def listener_thread():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('0.0.0.0', 9000))
    s.listen()
    print("Master listening on port 9000...")
    
    while True:
        conn, addr = s.accept()
        data = conn.recv(4096)
        if data:
            result = json.loads(data.decode())
            crawled_results.append(result)
            for link in result['links']:
                task_queue.put(link)
        conn.close()

def distribute_tasks():
    while True:
        if not task_queue.empty():
            url = task_queue.get()
            # Here, ideally you send URL to a free crawler
            print(f"Distribute: {url}")

def start_master(seed_urls):
    for url in seed_urls:
        task_queue.put(url)

    threading.Thread(target=listener_thread, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()

    while True:
        pass  # Keep main thread alive

if __name__ == "__main__":
    start_master(["https://example.com"])
