import redis
import json
import threading
import time
import sqlite3

# Initialize the SQLite database and table for crawled data
def create_database():
    conn = sqlite3.connect('crawler_data.db')
    c = conn.cursor()
    
    # Create a table to store crawled data
    c.execute('''CREATE TABLE IF NOT EXISTS crawled_data (
                 url TEXT PRIMARY KEY,
                 text TEXT,
                 links TEXT)''')
    conn.commit()
    conn.close()

# Function to save crawled data to the SQLite database
def save_data_to_db(url, text, links):
    conn = sqlite3.connect('crawler_data.db')
    c = conn.cursor()
    
    # Insert the crawled data into the database (use REPLACE to overwrite if URL exists)
    c.execute("INSERT OR REPLACE INTO crawled_data (url, text, links) VALUES (?, ?, ?)", 
              (url, text, ', '.join(links)))
    conn.commit()
    conn.close()

# Create the database on the first run
create_database()

# Connect to local Redis server
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Listener thread that receives crawled data and saves it to SQLite
def listener_thread():
    while True:
        # Poll Redis for completed crawls
        result = r.blpop("crawled_results", timeout=0)  # Block until data arrives
        if result:
            crawled_data = json.loads(result[1])  # Parse the JSON data from Redis
            print(f"Received data for URL: {crawled_data['url']}")
            url = crawled_data['url']
            text = crawled_data['text']
            links = crawled_data['links']
            
            # Save the fetched data to the SQLite database
            save_data_to_db(url, text, links)
            
            # Print received and saved data
            print(f"Saved data for URL: {url} to database.")
            
            # Distribute more URLs for crawling
            for link in crawled_data['links']:
                r.lpush("url_queue", link)  # Push new URLs to the task queue
            print(f"Distributing more URLs...")

# Function to distribute tasks by adding seed URLs to the Redis queue
def distribute_tasks():
    seed_urls = ["https://example.com", "https://example.org", "https://example.net"]
    for url in seed_urls:
        r.lpush("url_queue", url)
        print(f"Added seed URL to queue: {url}")
    
    # Continue adding URLs to the queue as they are discovered
    while True:
        time.sleep(1)

# Start the master node
def start_master():
    threading.Thread(target=listener_thread, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()
    while True:
        pass  # Keep the main thread alive to keep the listener running

if __name__ == "__main__":
    start_master()
