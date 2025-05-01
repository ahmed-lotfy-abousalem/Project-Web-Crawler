import requests
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1
import json
import threading
import time
import uuid
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('CrawlerNode')

# GCP configurations
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'web-crawler-458509')
NODE_ID = str(uuid.uuid4())
MASTER_URL = "http://34.173.15.32:5000"  # Replace with master VM's external IP

# Pub/Sub clients
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

# Pub/Sub topics
CRAWLED_RESULTS_TOPIC = f'projects/{PROJECT_ID}/topics/crawled-results'
HEARTBEATS_TOPIC = f'projects/{PROJECT_ID}/topics/heartbeats'
TASK_COMPLETE_TOPIC = f'projects/{PROJECT_ID}/topics/task-complete'
NODE_TASKS_TOPIC = f'projects/{PROJECT_ID}/topics/node-{NODE_ID}-tasks'
NODE_TASKS_SUB = f'projects/{PROJECT_ID}/subscriptions/node-{NODE_ID}-tasks'

def send_heartbeat():
    """Continuously send heartbeats to master"""
    while True:
        try:
            publisher.publish(
                HEARTBEATS_TOPIC,
                json.dumps({"node_id": NODE_ID}).encode('utf-8')
            )
            logger.debug("Heartbeat sent successfully")
        except Exception as e:
            logger.error(f"Heartbeat error: {str(e)}")
        time.sleep(3)

def fetch_page(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None

def extract_links(html_content, base_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    links = []
    for link_tag in soup.find_all('a', href=True):
        href = link_tag['href']
        if href.startswith('http'):
            links.append(href)
    return links

def extract_text(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    for script in soup(["script", "style"]):
        script.decompose()
    return ' '.join(soup.stripped_strings)

def send_result_to_master(crawled_data):
    try:
        publisher.publish(
            CRAWLED_RESULTS_TOPIC,
            json.dumps(crawled_data).encode('utf-8')
        )
        logger.info(f"Sent results for {crawled_data['url']}")
    except Exception as e:
        logger.error(f"Failed to send results: {e}")

def notify_task_completion(url):
    try:
        publisher.publish(
            TASK_COMPLETE_TOPIC,
            json.dumps({"node_id": NODE_ID, "url": url}).encode('utf-8')
        )
        logger.debug(f"Task completion notified for {url}")
    except Exception as e:
        logger.error(f"Task completion error: {str(e)}")

def crawl_task():
    # Create node-specific subscription
    try:
        subscriber.create_subscription(
            name=NODE_TASKS_SUB,
            topic=NODE_TASKS_TOPIC
        )
    except:
        logger.info(f"Subscription {NODE_TASKS_SUB} already exists")
    
    def callback(message):
        try:
            url = message.data.decode('utf-8')
            logger.info(f"Processing {url}")
            
            page = fetch_page(url)
            if page:
                text = extract_text(page)
                links = extract_links(page, url)
                data = {'url': url, 'text': text, 'links': links}
                send_result_to_master(data)
                notify_task_completion(url)
                logger.info(f"Completed processing {url} with {len(links)} links found")
            else:
                notify_task_completion(url)
                logger.warning(f"Failed to process {url}")
            message.ack()
        except Exception as e:
            logger.error(f"Crawler error: {e}")
            message.nack()
    
    subscriber.subscribe(NODE_TASKS_SUB, callback=callback)
    logger.info(f"Started crawler node {NODE_ID}")
    while True:
        time.sleep(1)

if __name__ == "__main__":
    # Create node-specific topic
    try:
        publisher.create_topic(name=NODE_TASKS_TOPIC)
    except:
        logger.info(f"Topic {NODE_TASKS_TOPIC} already exists")
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    
    # Start crawling
    crawl_task()