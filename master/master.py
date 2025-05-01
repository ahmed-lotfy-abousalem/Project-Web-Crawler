import redis
import json
import threading
import time
import sqlite3
import logging
from flask import Flask, request, jsonify
from logging.handlers import RotatingFileHandler

# Initialize Flask app
app = Flask(__name__)

# Configure logging
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
file_handler = RotatingFileHandler('master.log', maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(log_formatter)
logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
logger = logging.getLogger('MasterNode')

# Redis connection
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Global state
crawler_heartbeats = {}  # node_id -> last_seen timestamp
task_assignments = {}    # node_id -> list of assigned URLs
pending_tasks = set()    # URLs currently being processed
completed_tasks = set()  # URLs that have been processed

# Database setup
def create_database():
    conn = sqlite3.connect('data/crawler_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS crawled_data (
                 url TEXT PRIMARY KEY,
                 text TEXT,
                 links TEXT)''')
    conn.commit()
    conn.close()

def save_data_to_db(url, text, links):
    conn = sqlite3.connect('data/crawler_data.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO crawled_data (url, text, links) VALUES (?, ?, ?)", 
              (url, text, ', '.join(links)))
    conn.commit()
    conn.close()

# Initialize database
create_database()

# Flask endpoints
@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    node_id = request.json['node_id']
    crawler_heartbeats[node_id] = time.time()
    logger.info(f"Heartbeat received from {node_id}")
    return '', 200

@app.route('/task_complete', methods=['POST'])
def task_complete():
    data = request.json
    node_id = data['node_id']
    url = data['url']
    
    if node_id in task_assignments and url in task_assignments[node_id]:
        task_assignments[node_id].remove(url)
        completed_tasks.add(url)
        pending_tasks.discard(url)
        logger.info(f"Task completed: {url} by {node_id}")
    else:
        logger.warning(f"Unknown task completion: {url} by {node_id}")
    
    return '', 200

@app.route('/status')
def status():
    active_nodes = [n for n in crawler_heartbeats if time.time() - crawler_heartbeats[n] < 10]
    return jsonify({
        'active_nodes': active_nodes,
        'pending_tasks': len(pending_tasks),
        'completed_tasks': len(completed_tasks),
        'task_assignments': {n: len(t) for n, t in task_assignments.items()}
    })

# Worker functions
def listener_thread():
    while True:
        try:
            result = r.blpop("crawled_results", timeout=0)
            if result:
                crawled_data = json.loads(result[1])
                url = crawled_data['url']
                text = crawled_data['text']
                links = crawled_data['links']
                save_data_to_db(url, text, links)
                
                # Add new links to queue if not already processed
                for link in links:
                    if link not in completed_tasks and link not in pending_tasks:
                        r.lpush("url_queue", link)
                        pending_tasks.add(link)
                        logger.info(f"Discovered new URL: {link}")
        except Exception as e:
            logger.error(f"Listener error: {e}")

def distribute_tasks():
    # Initial seed URLs
    seed_urls = ["https://example.com", "https://example.org", "https://example.net"]
    for url in seed_urls:
        if url not in completed_tasks and url not in pending_tasks:
            r.lpush("url_queue", url)
            pending_tasks.add(url)
    
    while True:
        try:
            # Assign tasks to available nodes
            url = r.rpop("url_queue")
            if url:
                url = url.decode('utf-8')
                
                # Find available node (with fewest current assignments)
                available_nodes = [n for n in crawler_heartbeats 
                                 if time.time() - crawler_heartbeats[n] < 10]
                
                if available_nodes:
                    # Assign to node with fewest tasks
                    node_id = min(available_nodes, 
                                 key=lambda n: len(task_assignments.get(n, [])))
                    
                    if node_id not in task_assignments:
                        task_assignments[node_id] = []
                    
                    task_assignments[node_id].append(url)
                    r.lpush(f"node:{node_id}:tasks", url)
                    logger.info(f"Assigned {url} to {node_id}")
                else:
                    # No available nodes, requeue
                    r.lpush("url_queue", url)
                    logger.warning("No available nodes for task assignment")
            
            time.sleep(1)
        except Exception as e:
            logger.error(f"Distributor error: {e}")

def monitor_heartbeat():
    while True:
        try:
            now = time.time()
            dead_nodes = []
            
            for node_id, last_seen in list(crawler_heartbeats.items()):
                if now - last_seen > 10:  # 10 second timeout
                    dead_nodes.append(node_id)
                    logger.warning(f"Node {node_id} failed (last seen: {time.ctime(last_seen)})")
                    
                    # Requeue all tasks from dead node
                    if node_id in task_assignments:
                        requeued = 0
                        for url in task_assignments[node_id]:
                            if url not in completed_tasks:
                                r.lpush("url_queue", url)
                                requeued += 1
                                logger.info(f"Re-queued task: {url}")
                        
                        logger.critical(f"Re-queued {requeued} tasks from dead node {node_id}")
                        del task_assignments[node_id]
                    
                    del crawler_heartbeats[node_id]
            
            time.sleep(5)
        except Exception as e:
            logger.error(f"Heartbeat monitor error: {e}")

def start_master():
    # Start worker threads
    threading.Thread(target=listener_thread, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()
    threading.Thread(target=monitor_heartbeat, daemon=True).start()
    
    # Start Flask app
    app.run(host='0.0.0.0', port=5000)

if __name__ == "__main__":
    start_master()