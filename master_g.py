import threading
import time
import logging
from flask import Flask, request, jsonify
from logging.handlers import RotatingFileHandler
from google.cloud import pubsub_v1
import json
import sqlite3
import os

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

# GCP configurations
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'web-crawler-458509')

# Pub/Sub clients
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

# Pub/Sub topics and subscriptions
URL_QUEUE_TOPIC = f'projects/{PROJECT_ID}/topics/url-queue'
CRAWLED_RESULTS_TOPIC = f'projects/{PROJECT_ID}/topics/crawled-results'
HEARTBEATS_TOPIC = f'projects/{PROJECT_ID}/topics/heartbeats'
TASK_COMPLETE_TOPIC = f'projects/{PROJECT_ID}/topics/task-complete'
CRAWLED_RESULTS_SUB = f'projects/{PROJECT_ID}/subscriptions/crawled-results-sub'
HEARTBEATS_SUB = f'projects/{PROJECT_ID}/subscriptions/heartbeats-sub'
TASK_COMPLETE_SUB = f'projects/{PROJECT_ID}/subscriptions/task-complete-sub'

# Global state
crawler_heartbeats = {}  # node_id -> last_seen timestamp
task_assignments = {}    # node_id -> list of assigned URLs
pending_tasks = set()    # URLs currently being processed
completed_tasks = set()  # URLs that have been processed

# Database setup
def create_database():
    if not os.path.exists('data'):
        os.makedirs('data')
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
    def callback(message):
        try:
            crawled_data = json.loads(message.data.decode('utf-8'))
            url = crawled_data['url']
            text = crawled_data['text']
            links = crawled_data['links']
            save_data_to_db(url, text, links)
            
            # Add new links to queue if not already processed
            for link in links:
                if link not in completed_tasks and link not in pending_tasks:
                    publisher.publish(URL_QUEUE_TOPIC, link.encode('utf-8'))
                    pending_tasks.add(link)
                    logger.info(f"Discovered new URL: {link}")
            message.ack()
        except Exception as e:
            logger.error(f"Listener error: {e}")
            message.nack()

    subscriber.subscribe(CRAWLED_RESULTS_SUB, callback=callback)
    logger.info("Started crawled results listener")
    while True:
        time.sleep(1)

def task_complete_listener():
    def callback(message):
        try:
            data = json.loads(message.data.decode('utf-8'))
            node_id = data['node_id']
            url = data['url']
            if node_id in task_assignments and url in task_assignments[node_id]:
                task_assignments[node_id].remove(url)
                completed_tasks.add(url)
                pending_tasks.discard(url)
                logger.info(f"Task completed: {url} by {node_id}")
            else:
                logger.warning(f"Unknown task completion: {url} by {node_id}")
            message.ack()
        except Exception as e:
            logger.error(f"Task complete listener error: {e}")
            message.nack()

    subscriber.subscribe(TASK_COMPLETE_SUB, callback=callback)
    logger.info("Started task complete listener")
    while True:
        time.sleep(1)

def distribute_tasks():
    # Initial seed URLs
    seed_urls = ["https://example.com", "https://example.org", "https://example.net"]
    for url in seed_urls:
        if url not in completed_tasks and url not in pending_tasks:
            publisher.publish(URL_QUEUE_TOPIC, url.encode('utf-8'))
            pending_tasks.add(url)
    
    while True:
        try:
            # Pull URLs from url-queue
            subscription_path = f'projects/{PROJECT_ID}/subscriptions/url-queue-sub'
            response = subscriber.pull(
                subscription=subscription_path,
                max_messages=1,
                return_immediately=True
            )
            
            for received_message in response.received_messages:
                url = received_message.message.data.decode('utf-8')
                subscriber.acknowledge(
                    subscription=subscription_path,
                    ack_ids=[received_message.ack_id]
                )
                
                # Find available node
                available_nodes = [n for n in crawler_heartbeats 
                                 if time.time() - crawler_heartbeats[n] < 10]
                
                if available_nodes:
                    node_id = min(available_nodes, 
                                 key=lambda n: len(task_assignments.get(n, [])))
                    
                    if node_id not in task_assignments:
                        task_assignments[node_id] = []
                    
                    task_assignments[node_id].append(url)
                    node_topic = f'projects/{PROJECT_ID}/topics/node-{node_id}-tasks'
                    publisher.publish(node_topic, url.encode('utf-8'))
                    logger.info(f"Assigned {url} to {node_id}")
                else:
                    # Requeue if no nodes available
                    publisher.publish(URL_QUEUE_TOPIC, url.encode('utf-8'))
                    logger.warning("No available nodes for task assignment")
            
            time.sleep(1)
        except Exception as e:
            logger.error(f"Distributor error: {e}")
            time.sleep(5)

def monitor_heartbeat():
    def callback(message):
        try:
            data = json.loads(message.data.decode('utf-8'))
            node_id = data['node_id']
            crawler_heartbeats[node_id] = time.time()
            logger.info(f"Heartbeat received from {node_id} via Pub/Sub")
            message.ack()
        except Exception as e:
            logger.error(f"Heartbeat callback error: {e}")
            message.nack()

    subscriber.subscribe(HEARTBEATS_SUB, callback=callback)
    logger.info("Started heartbeat listener")
    
    while True:
        try:
            now = time.time()
            for node_id, last_seen in list(crawler_heartbeats.items()):
                if now - last_seen > 10:
                    logger.warning(f"Node {node_id} failed (last seen: {time.ctime(last_seen)})")
                    if node_id in task_assignments:
                        requeued = 0
                        for url in task_assignments[node_id]:
                            if url not in completed_tasks:
                                publisher.publish(URL_QUEUE_TOPIC, url.encode('utf-8'))
                                requeued += 1
                                logger.info(f"Re-queued task: {url}")
                        logger.critical(f"Re-queued {requeued} tasks from dead node {node_id}")
                        del task_assignments[node_id]
                    del crawler_heartbeats[node_id]
            time.sleep(5)
        except Exception as e:
            logger.error(f"Heartbeat monitor error: {e}")

def start_master():
    # Create url-queue subscription
    subscription_path = f'projects/{PROJECT_ID}/subscriptions/url-queue-sub'
    try:
        subscriber.create_subscription(
            name=subscription_path,
            topic=URL_QUEUE_TOPIC
        )
    except:
        logger.info("url-queue-sub already exists")
    
    # Start worker threads
    threading.Thread(target=listener_thread, daemon=True).start()
    threading.Thread(target=task_complete_listener, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()
    threading.Thread(target=monitor_heartbeat, daemon=True).start()
    
    # Start Flask app
    app.run(host='0.0.0.0', port=5000)

if __name__ == "__main__":
    start_master()