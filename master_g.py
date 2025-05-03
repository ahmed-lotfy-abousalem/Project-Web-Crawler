import json
import logging
from logging.handlers import RotatingFileHandler
import threading
import time
from google.cloud import pubsub_v1

# Configure logging
log_formatter = logging.Formatter('%(asctime)s - Master - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
file_handler = RotatingFileHandler('master.log', maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(log_formatter)
logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
logger = logging.getLogger('Master')

# Pub/Sub setup
PROJECT_ID = "web-crawler-458509"  # Replace with your project ID
CRAWL_TOPIC_NAME = "crawl-tasks-topic"
CRAWL_SUBSCRIPTION_NAME = "crawl-tasks-topic-sub"
INDEXING_TOPIC_NAME = "indexing-tasks-topic"
HEARTBEATS_TOPIC_NAME = "heartbeats"
TASK_COMPLETE_TOPIC_NAME = "task-complete"
CRAWLED_RESULTS_TOPIC_NAME = "crawled-results"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
crawl_topic_path = publisher.topic_path(PROJECT_ID, CRAWL_TOPIC_NAME)
crawl_subscription_path = subscriber.subscription_path(PROJECT_ID, CRAWL_SUBSCRIPTION_NAME)
indexing_topic_path = publisher.topic_path(PROJECT_ID, INDEXING_TOPIC_NAME)
heartbeats_topic_path = publisher.topic_path(PROJECT_ID, HEARTBEATS_TOPIC_NAME)
task_complete_topic_path = publisher.topic_path(PROJECT_ID, TASK_COMPLETE_TOPIC_NAME)
crawled_results_topic_path = publisher.topic_path(PROJECT_ID, CRAWLED_RESULTS_TOPIC_NAME)
crawled_results_subscription_path = subscriber.subscription_path(PROJECT_ID, "crawled-results-sub")
heartbeats_subscription_path = subscriber.subscription_path(PROJECT_ID, "heartbeats-sub")
task_complete_subscription_path = subscriber.subscription_path(PROJECT_ID, "task-complete-sub")

# Global state
crawler_heartbeats = {}  # node_id -> last_seen timestamp
task_assignments = {}    # node_id -> list of assigned URLs
pending_tasks = set()    # URLs currently being processed
completed_tasks = set()  # URLs that have been processed

def crawled_results_listener():
    def callback(message):
        try:
            crawled_data = json.loads(message.data.decode('utf-8'))
            url = crawled_data['url']
            gcs_path = crawled_data['gcs_path']
            
            # Log the received data (no database, as we're using GCS)
            logger.info(f"Received crawled data for {url} stored at {gcs_path}")
            
            # Add new URLs to queue if not already processed
            for link in crawled_data.get('links', []):
                if link not in completed_tasks and link not in pending_tasks:
                    publisher.publish(crawl_topic_path, json.dumps({
                        "url": link,
                        "max_depth": crawled_data.get('max_depth', 2),  # Default max_depth
                        "current_depth": crawled_data.get('current_depth', 1) + 1
                    }).encode('utf-8'))
                    pending_tasks.add(link)
                    logger.info(f"Discovered new URL: {link}")
            message.ack()
        except Exception as e:
            logger.error(f"Crawled results listener error: {e}")
            message.nack()

    subscriber.subscribe(crawled_results_subscription_path, callback=callback)
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

    subscriber.subscribe(task_complete_subscription_path, callback=callback)
    logger.info("Started task complete listener")
    while True:
        time.sleep(1)

def distribute_tasks():
    # Initial seed URLs
    seed_urls = [
        {"url": "https://example.com", "max_depth": 3},
        {"url": "https://example.org", "max_depth": 2},
        {"url": "https://example.net", "max_depth": 2}
    ]
    for task in seed_urls:
        if task["url"] not in completed_tasks and task["url"] not in pending_tasks:
            task["current_depth"] = 1
            publisher.publish(crawl_topic_path, json.dumps(task).encode('utf-8'))
            pending_tasks.add(task["url"])
            logger.info(f"Seeded URL: {task['url']}")
    
    while True:
        try:
            # Pull URLs from crawl queue
            response = subscriber.pull(
                request={"subscription": crawl_subscription_path, "max_messages": 1},
                timeout=5
            )
            
            for received_message in response.received_messages:
                task = json.loads(received_message.message.data.decode('utf-8'))
                url = task["url"]
                max_depth = task["max_depth"]
                current_depth = task.get("current_depth", 1)
                subscriber.acknowledge(
                    request={"subscription": crawl_subscription_path, "ack_ids": [received_message.ack_id]}
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
                    publisher.publish(crawl_topic_path, json.dumps({
                        "url": url,
                        "max_depth": max_depth,
                        "current_depth": current_depth
                    }).encode('utf-8'))
                    logger.info(f"Assigned {url} to {node_id}")
                else:
                    # Requeue if no nodes available
                    publisher.publish(crawl_topic_path, json.dumps({
                        "url": url,
                        "max_depth": max_depth,
                        "current_depth": current_depth
                    }).encode('utf-8'))
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

    subscriber.subscribe(heartbeats_subscription_path, callback=callback)
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
                                publisher.publish(crawl_topic_path, json.dumps({
                                    "url": url,
                                    "max_depth": 2,  # Default max_depth for re-queued tasks
                                    "current_depth": 1  # Reset depth for simplicity
                                }).encode('utf-8'))
                                requeued += 1
                                logger.info(f"Re-queued task: {url}")
                        logger.critical(f"Re-queued {requeued} tasks from dead node {node_id}")
                        del task_assignments[node_id]
                    del crawler_heartbeats[node_id]
            time.sleep(5)
        except Exception as e:
            logger.error(f"Heartbeat monitor error: {e}")

def start_master():
    # Create topics
    for topic in [crawl_topic_path, indexing_topic_path, heartbeats_topic_path, 
                  task_complete_topic_path, crawled_results_topic_path]:
        try:
            publisher.create_topic(request={"name": topic})
            logger.info(f"Created topic {topic}")
        except:
            logger.info(f"Topic {topic} already exists")
    
    # Create subscriptions
    for sub, topic in [
        (crawl_subscription_path, crawl_topic_path),
        (crawled_results_subscription_path, crawled_results_topic_path),
        (heartbeats_subscription_path, heartbeats_topic_path),
        (task_complete_subscription_path, task_complete_topic_path)
    ]:
        try:
            subscriber.create_subscription(request={"name": sub, "topic": topic})
            logger.info(f"Created subscription {sub}")
        except:
            logger.info(f"Subscription {sub} already exists")
    
    # Start worker threads
    threading.Thread(target=crawled_results_listener, daemon=True).start()
    threading.Thread(target=task_complete_listener, daemon=True).start()
    threading.Thread(target=distribute_tasks, daemon=True).start()
    threading.Thread(target=monitor_heartbeat, daemon=True).start()
    
    logger.info("Master node started")
    while True:
        time.sleep(60)  # Keep the main thread alive

if __name__ == '__main__':
    start_master()