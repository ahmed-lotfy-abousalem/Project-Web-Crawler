import redis
import json
import threading
import time

# Connect to local Redis server
r = redis.StrictRedis(host='localhost', port=6379, db=0)

def listener_thread():
    while True:
        # Poll Redis for completed crawls
        result = r.blpop("crawled_results", timeout=0)  # block until data arrives
        if result:
            crawled_data = json.loads(result[1])
            print(f"Received data for URL: {crawled_data['url']}")
            # In a full implementation, this is where you'd add the data to an index
            # For now, we'll just print it.
            for link in crawled_data['links']:
                r.lpush("url_queue", link)  # Push new URLs back to task queue
            print(f"Distributing more URLs...")

def distribute_tasks():
    # Start with some seed URLs
     seed_urls = ["https://example.com", "https://example.org", "https://example.net"]
     for url in seed_urls:
        r.lpush("url_queue", url)
        print(f"Added seed URL to queue: {url}")
    
     while True:
        time.sleep(1)

def start_master():
    threading.Thread(target=listener_thread, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()
    while True:
        pass  # Keep main thread alive to keep listener running

if __name__ == "__main__":
    start_master()
