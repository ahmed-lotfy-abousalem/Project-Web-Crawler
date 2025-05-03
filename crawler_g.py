import json
import logging
from logging.handlers import RotatingFileHandler
import threading
import time
import uuid
from google.cloud import pubsub_v1
from google.cloud import storage
import scrapy
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse

# Configure logging
log_formatter = logging.Formatter('%(asctime)s - Crawler - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
file_handler = RotatingFileHandler('crawler.log', maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(log_formatter)
logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
logger = logging.getLogger('Crawler')

# Pub/Sub setup
PROJECT_ID = "web-crawler-458509"  # Replace with your project ID
NODE_ID = str(uuid.uuid4())
CRAWL_TOPIC_NAME = "crawl-tasks-topic"
CRAWL_SUBSCRIPTION_NAME = "crawl-tasks-topic-sub"
INDEXING_TOPIC_NAME = "indexing-tasks-topic"
HEARTBEATS_TOPIC_NAME = "heartbeats"
TASK_COMPLETE_TOPIC_NAME = "task-complete"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
crawl_topic_path = publisher.topic_path(PROJECT_ID, CRAWL_TOPIC_NAME)
crawl_subscription_path = subscriber.subscription_path(PROJECT_ID, CRAWL_SUBSCRIPTION_NAME)
indexing_topic_path = publisher.topic_path(PROJECT_ID, INDEXING_TOPIC_NAME)
heartbeats_topic_path = publisher.topic_path(PROJECT_ID, HEARTBEATS_TOPIC_NAME)
task_complete_topic_path = publisher.topic_path(PROJECT_ID, TASK_COMPLETE_TOPIC_NAME)

# Google Cloud Storage setup
storage_client = storage.Client()
BUCKET_NAME = "abousalem1"  # Replace with your GCS bucket name
bucket = storage_client.bucket(BUCKET_NAME)

# Global sets to track crawled URLs and their depths
crawled_urls = set()  # To prevent cyclic crawling
url_depths = {}  # To track the depth of each URL

def send_heartbeat():
    """Continuously send heartbeats to master via Pub/Sub"""
    while True:
        try:
            publisher.publish(
                heartbeats_topic_path,
                json.dumps({"node_id": NODE_ID}).encode('utf-8')
            )
            logger.debug("Heartbeat sent successfully")
        except Exception as e:
            logger.error(f"Heartbeat error: {str(e)}")
        time.sleep(3)

class CrawlerSpider(scrapy.Spider):
    name = "crawler_spider"
    
    def __init__(self, url_to_crawl, current_depth, max_depth, *args, **kwargs):
        super(CrawlerSpider, self).__init__(*args, **kwargs)
        self.start_urls = [url_to_crawl]
        self.current_depth = current_depth
        self.max_depth = max_depth
    
    def parse(self, response):
        logger.info(f"Crawling {response.url} at depth {self.current_depth}")
        
        # Mark this URL as crawled
        crawled_urls.add(response.url)
        
        # Extract content (text)
        content = response.css('body').get()
        if content:
            content = content.encode('utf-8')
            blob_name = f"crawled/{response.url.replace('/', '_')}.html"
            blob = bucket.blob(blob_name)
            blob.upload_from_string(content)
            logger.info(f"Stored content for {response.url} in GCS")
            
            indexing_task = {
                "url": response.url,
                "gcs_path": f"gs://{BUCKET_NAME}/{blob_name}"
            }
            publisher.publish(indexing_topic_path, json.dumps(indexing_task).encode('utf-8'))
            logger.info(f"Published indexing task for {response.url}")
        
        # Extract new URLs if we're not at max depth
        if self.current_depth < self.max_depth:
            new_urls = response.css('a::attr(href)').getall()
            new_urls = [response.urljoin(url) for url in new_urls if url.startswith('http')]
            
            next_depth = self.current_depth + 1
            for url in new_urls:
                if url not in crawled_urls:
                    url_depths[url] = next_depth
                    publisher.publish(crawl_topic_path, json.dumps({
                        "url": url,
                        "max_depth": self.max_depth,
                        "current_depth": next_depth
                    }).encode('utf-8'))
            logger.info(f"Published {len(new_urls)} new URLs at depth {next_depth} to crawl queue")
        
        # Notify task completion
        try:
            publisher.publish(
                task_complete_topic_path,
                json.dumps({"node_id": NODE_ID, "url": response.url}).encode('utf-8')
            )
            logger.debug(f"Task completion notified for {response.url}")
        except Exception as e:
            logger.error(f"Task completion error: {str(e)}")

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
    except ValueError:
        return False

def crawl_url(url_to_crawl, current_depth, max_depth):
    """Run Scrapy crawling in the main thread."""
    logger.info(f"Starting crawl for {url_to_crawl} at depth {current_depth}")
    process = CrawlerProcess(settings={
        'DOWNLOAD_DELAY': 2,
        'ROBOTSTXT_OBEY': True,
        'LOG_LEVEL': 'INFO',
        'TELNETCONSOLE_ENABLED': False,
    })
    process.crawl(CrawlerSpider, url_to_crawl=url_to_crawl, current_depth=current_depth, max_depth=max_depth)
    process.start()
    logger.info(f"Completed crawling {url_to_crawl} at depth {current_depth}")

def crawler_process():
    # Create topics and subscription if they don't exist
    for topic in [crawl_topic_path, indexing_topic_path, heartbeats_topic_path, task_complete_topic_path]:
        try:
            publisher.create_topic(request={"name": topic})
            logger.info(f"Created topic {topic}")
        except:
            logger.info(f"Topic {topic} already exists")
    
    try:
        subscriber.create_subscription(
            request={"name": crawl_subscription_path, "topic": crawl_topic_path}
        )
        logger.info(f"Created subscription {crawl_subscription_path}")
    except:
        logger.info(f"Subscription {crawl_subscription_path} already exists")
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    
    logger.info(f"Crawler node {NODE_ID} started")
    logger.info(f"Listening for crawl tasks on {crawl_subscription_path}...")
    
    while True:
        # Pull messages synchronously in the main thread
        response = subscriber.pull(
            request={"subscription": crawl_subscription_path, "max_messages": 1},
            timeout=60
        )
        
        if not response.received_messages:
            logger.info("No messages received, waiting...")
            # Reset state if no messages are pending
            if not url_depths:
                crawled_urls.clear()
                logger.info("No pending URLs to crawl, reset crawled_urls set.")
            continue
        
        for message in response.received_messages:
            try:
                task = json.loads(message.message.data.decode('utf-8'))
                url_to_crawl = task["url"]
                max_depth = task["max_depth"]
                current_depth = task.get("current_depth", 1)  # Default to 1 for root URL
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Invalid message format: {e}")
                subscriber.acknowledge(
                    request={"subscription": crawl_subscription_path, "ack_ids": [message.ack_id]}
                )
                continue
            
            logger.info(f"Received URL to crawl: {url_to_crawl} at depth {current_depth} with max depth {max_depth}")
            
            if not is_valid_url(url_to_crawl):
                logger.error(f"Invalid URL: {url_to_crawl}")
                subscriber.acknowledge(
                    request={"subscription": crawl_subscription_path, "ack_ids": [message.ack_id]}
                )
                continue
            
            if url_to_crawl in crawled_urls:
                logger.info(f"URL already crawled: {url_to_crawl}, skipping...")
                subscriber.acknowledge(
                    request={"subscription": crawl_subscription_path, "ack_ids": [message.ack_id]}
                )
                url_depths.pop(url_to_crawl, None)
                continue
            
            if current_depth > max_depth:
                logger.info(f"Depth {current_depth} exceeds max depth {max_depth} for {url_to_crawl}, skipping...")
                subscriber.acknowledge(
                    request={"subscription": crawl_subscription_path, "ack_ids": [message.ack_id]}
                )
                url_depths.pop(url_to_crawl, None)
                continue
            
            try:
                # Crawl in the main thread
                crawl_url(url_to_crawl, current_depth, max_depth)
                
                # Acknowledge the message
                subscriber.acknowledge(
                    request={"subscription": crawl_subscription_path, "ack_ids": [message.ack_id]}
                )
                logger.info(f"Acknowledged message for {url_to_crawl}")
                
                # Remove from url_depths since we're done with this URL
                url_depths.pop(url_to_crawl, None)
            except Exception as e:
                logger.error(f"Error crawling {url_to_crawl}: {e}")
                # Nack the message to retry
                subscriber.modify_ack_deadline(
                    request={
                        "subscription": crawl_subscription_path,
                        "ack_ids": [message.ack_id],
                        "ack_deadline_seconds": 0,
                    }
                )

if __name__ == '__main__':
    crawler_process()