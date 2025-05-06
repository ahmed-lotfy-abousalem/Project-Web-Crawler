import json
import logging
from logging.handlers import RotatingFileHandler
import threading
import time
import uuid
from google.cloud import pubsub_v1
from google.cloud import storage
import scrapy
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
from twisted.internet.defer import Deferred
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
PROJECT_ID = "web-crawler-458509"
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
BUCKET_NAME = "abousalem1"
bucket = storage_client.bucket(BUCKET_NAME)

# Global sets to track crawled URLs and their depths
crawled_urls = set()
url_depths = {}

def send_heartbeat():
    """Continuously send heartbeats to master via Pub/Sub"""
    while True:
        try:
            publisher.publish(
                heartbeats_topic_path,
                json.dumps({"node_id": NODE_ID}).encode('utf-8')
            )
            logger.debug("Heartbeat sent for node %s", NODE_ID)
        except Exception as e:
            logger.error("Heartbeat error: %s", str(e))
        time.sleep(3)

class CrawlerSpider(scrapy.Spider):
    name = "crawler_spider"
    
    def __init__(self, url_to_crawl, current_depth, max_depth, *args, **kwargs):
        super(CrawlerSpider, self).__init__(*args, **kwargs)
        self.start_urls = [url_to_crawl]
        self.current_depth = current_depth
        self.max_depth = max_depth
    
    def parse(self, response):
        logger.info("Crawling %s at depth %s", response.url, self.current_depth)
        
        crawled_urls.add(response.url)
        
        content = response.css('body').get()
        if content:
            content = content.encode('utf-8')
            blob_name = f"crawled/{response.url.replace('/', '_')}.html"
            blob = bucket.blob(blob_name)
            blob.upload_from_string(content)
            logger.info("Stored content for %s in GCS", response.url)
            
            indexing_task = {
                "url": response.url,
                "gcs_path": f"gs://{BUCKET_NAME}/{blob_name}"
            }
            publisher.publish(indexing_topic_path, json.dumps(indexing_task).encode('utf-8'))
            logger.info("Published indexing task for %s", response.url)
        
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
            logger.info("Published %s new URLs at depth %s to crawl queue", len(new_urls), next_depth)
        
        try:
            publisher.publish(
                task_complete_topic_path,
                json.dumps({"node_id": NODE_ID, "url": response.url}).encode('utf-8')
            )
            logger.debug("Task completion notified for %s", response.url)
        except Exception as e:
            logger.error("Task completion error: %s", str(e))

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https']
    except ValueError:
        return False

@defer.inlineCallbacks
def crawl_url(url_to_crawl, current_depth, max_depth, runner, message, subscription_path):
    """Schedule a crawl using CrawlerRunner and handle message acknowledgment."""
    logger.info("Starting crawl for %s at depth %s", url_to_crawl, current_depth)
    deferred = runner.crawl(CrawlerSpider, url_to_crawl=url_to_crawl, current_depth=current_depth, max_depth=max_depth)
    yield deferred
    logger.info("Completed crawling %s at depth %s", url_to_crawl, current_depth)
    
    # Acknowledge the message after crawl completes
    subscriber.acknowledge(
        request={"subscription": subscription_path, "ack_ids": [message.ack_id]}
    )
    logger.info("Acknowledged message for %s", url_to_crawl)

def crawler_process():
    # Create topics and subscription if they don't exist
    for topic in [crawl_topic_path, indexing_topic_path, heartbeats_topic_path, task_complete_topic_path]:
        try:
            publisher.create_topic(request={"name": topic})
            logger.info("Created topic %s", topic)
        except:
            logger.info("Topic %s already exists", topic)
    
    try:
        subscriber.create_subscription(
            request={"name": crawl_subscription_path, "topic": crawl_topic_path}
        )
        logger.info("Created subscription %s", crawl_subscription_path)
    except:
        logger.info("Subscription %s already exists", crawl_subscription_path)
    
    # Initialize CrawlerRunner
    runner = CrawlerRunner(get_project_settings())
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    
    # Start reactor in a separate thread
    def run_reactor():
        reactor.run(installSignalHandlers=False)
    
    reactor_thread = threading.Thread(target=run_reactor, daemon=True)
    reactor_thread.start()
    
    logger.info("Crawler node %s started", NODE_ID)
    logger.info("Listening for crawl tasks on %s...", crawl_subscription_path)
    
    while True:
        # Pull messages synchronously in the main thread
        response = subscriber.pull(
            request={"subscription": crawl_subscription_path, "max_messages": 1},
            timeout=60
        )
        
        if not response.received_messages:
            logger.info("No messages received, waiting...")
            if not url_depths:
                crawled_urls.clear()
                logger.info("No pending URLs to crawl, reset crawled_urls set.")
            continue
        
        for message in response.received_messages:
            try:
                task = json.loads(message.message.data.decode('utf-8'))
                url_to_crawl = task["url"]
                max_depth = task["max_depth"]
                current_depth = task.get("current_depth", 1)
            except (json.JSONDecodeError, KeyError) as e:
                logger.error("Invalid message format: %s", e)
                subscriber.acknowledge(
                    request={"subscription": crawl_subscription_path, "ack_ids": [message.ack_id]}
                )
                continue
            
            logger.info("Received URL to crawl: %s at depth %s with max depth %s", url_to_crawl, current_depth, max_depth)
            
            if not is_valid_url(url_to_crawl):
                logger.error("Invalid URL: %s", url_to_crawl)
                subscriber.acknowledge(
                    request={"subscription": crawl_subscription_path, "ack_ids": [message.ack_id]}
                )
                url_depths.pop(url_to_crawl, None)
                continue
            
            if url_to_crawl in crawled_urls:
                logger.info("URL already crawled: %s, skipping...", url_to_crawl)
                subscriber.acknowledge(
                    request={"subscription": crawl_subscription_path, "ack_ids": [message.ack_id]}
                )
                url_depths.pop(url_to_crawl, None)
                continue
            
            if current_depth > max_depth:
                logger.info("Depth %s exceeds max depth %s for %s, skipping...", current_depth, max_depth, url_to_crawl)
                subscriber.acknowledge(
                    request={"subscription": crawl_subscription_path, "ack_ids": [message.ack_id]}
                )
                url_depths.pop(url_to_crawl, None)
                continue
            
            try:
                # Schedule the crawl with CrawlerRunner
                reactor.callFromThread(crawl_url, url_to_crawl, current_depth, max_depth, runner, message, crawl_subscription_path)
                # Remove from url_depths after scheduling
                url_depths.pop(url_to_crawl, None)
            except Exception as e:
                logger.error("Error scheduling crawl for %s: %s", url_to_crawl, e)
                subscriber.modify_ack_deadline(
                    request={
                        "subscription": crawl_subscription_path,
                        "ack_ids": [message.ack_id],
                        "ack_deadline_seconds": 0,
                    }
                )

if __name__ == '__main__':
    crawler_process()