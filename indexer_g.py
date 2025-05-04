import json
import logging
import traceback
from logging.handlers import RotatingFileHandler
import threading
import time
import uuid
import re
from google.cloud import pubsub_v1
from google.cloud import storage
from lxml import html
from google.api_core.exceptions import NotFound, AlreadyExists, PermissionDenied

# Configure logging
log_formatter = logging.Formatter('%(asctime)s - Indexer - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
handlers = [console_handler]

try:
    file_handler = RotatingFileHandler('indexer.log', maxBytes=5*1024*1024, backupCount=3)
    file_handler.setFormatter(log_formatter)
    handlers.append(file_handler)
except PermissionError as e:
    print(f"Warning: Cannot write to indexer.log due to permission error: {e}")
    print("Logging to console only.")

logging.basicConfig(level=logging.INFO, handlers=handlers)
logging.getLogger('google.cloud.pubsub_v1').setLevel(logging.DEBUG)
logger = logging.getLogger('Indexer')

# Pub/Sub setup
PROJECT_ID = "web-crawler-458509"
NODE_ID = str(uuid.uuid4())
INDEXING_TOPIC_NAME = "indexing-tasks-topic"
INDEXING_SUBSCRIPTION_NAME = "indexing-tasks-topic-sub"
HEARTBEATS_TOPIC_NAME = "heartbeats"
TASK_COMPLETE_TOPIC_NAME = "task-complete"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
indexing_topic_path = publisher.topic_path(PROJECT_ID, INDEXING_TOPIC_NAME)
indexing_subscription_path = subscriber.subscription_path(PROJECT_ID, INDEXING_SUBSCRIPTION_NAME)
heartbeats_topic_path = publisher.topic_path(PROJECT_ID, HEARTBEATS_TOPIC_NAME)
task_complete_topic_path = publisher.topic_path(PROJECT_ID, TASK_COMPLETE_TOPIC_NAME)

# Google Cloud Storage setup
try:
    storage_client = storage.Client()
    BUCKET_NAME = "abousalem1"
    bucket = storage_client.bucket(BUCKET_NAME)
    # Test bucket access
    test_blob = bucket.blob("test_access.txt")
    test_blob.upload_from_string("Test", content_type="text/plain")
    test_blob.delete()
    logger.info("Verified GCS bucket access")
except Exception as e:
    logger.critical(f"Failed to access GCS bucket {BUCKET_NAME}: {e}")
    raise

# Inverted index to store word -> {url: term_frequency}
inverted_index = {}

def send_heartbeat():
    while True:
        try:
            publisher.publish(
                heartbeats_topic_path,
                json.dumps({"node_id": NODE_ID}).encode('utf-8')
            )
            logger.debug(f"Heartbeat sent for node {NODE_ID}")
        except PermissionDenied as e:
            logger.error(f"Permission denied publishing heartbeat: {e}")
        except Exception as e:
            logger.error(f"Heartbeat error: {str(e)}")
        time.sleep(3)

def tokenize(text):
    """Tokenize text into words, removing punctuation and converting to lowercase."""
    # Remove non-alphanumeric characters and split into words
    words = re.findall(r'\b\w+\b', text.lower())
    return [word for word in words if word and len(word) > 1]  # Filter out single characters

def process_content(url, gcs_path):
    """Process HTML content from GCS, update inverted index, and store in GCS."""
    try:
        # Extract blob name from gcs_path (e.g., gs://abousalem1/crawled/...)
        blob_name = gcs_path.replace(f"gs://{BUCKET_NAME}/", "")
        blob = bucket.blob(blob_name)
        
        # Download HTML content
        content = blob.download_as_bytes()
        logger.info(f"Downloaded content for {url} from {gcs_path}")
        
        # Parse HTML and extract text
        tree = html.fromstring(content)
        text = ' '.join(tree.xpath('//text()')).strip()
        if not text:
            logger.warning(f"No text content extracted for {url}")
            return
        
        # Tokenize text
        words = tokenize(text)
        if not words:
            logger.warning(f"No valid words tokenized for {url}")
            return
        
        # Calculate term frequency
        term_freq = {}
        for word in words:
            term_freq[word] = term_freq.get(word, 0) + 1
        
        # Update inverted index
        for word, freq in term_freq.items():
            if word not in inverted_index:
                inverted_index[word] = {}
            inverted_index[word][url] = freq
        logger.info(f"Updated inverted index for {url} with {len(term_freq)} terms")
        
        # Store inverted index in GCS
        index_blob_name = f"index/inverted_index_{NODE_ID}.json"
        index_blob = bucket.blob(index_blob_name)
        index_blob.upload_from_string(
            json.dumps(inverted_index, indent=2),
            content_type="application/json"
        )
        logger.info(f"Stored inverted index in GCS at gs://{BUCKET_NAME}/{index_blob_name}")
        
    except NotFound as e:
        logger.error(f"GCS blob {blob_name} not found: {e}")
    except PermissionDenied as e:
        logger.error(f"Permission denied accessing GCS: {e}")
    except Exception as e:
        logger.error(f"Error processing {url} from {gcs_path}: {traceback.format_exc()}")

def indexer_process():
    """Main indexer process to listen for indexing tasks and process them."""
    # Create or verify Pub/Sub topics
    for topic in [indexing_topic_path, heartbeats_topic_path, task_complete_topic_path]:
        try:
            publisher.create_topic(request={"name": topic})
            logger.info(f"Created topic {topic}")
        except AlreadyExists:
            logger.info(f"Topic {topic} already exists")
        except PermissionDenied as e:
            logger.warning(f"Permission denied creating topic {topic}: {e}. Assuming it exists.")
        except Exception as e:
            logger.error(f"Error creating topic {topic}: {e}")
    
    # Create or verify indexing subscription with retries
    for attempt in range(5):
        try:
            subscriber.create_subscription(
                request={"name": indexing_subscription_path, "topic": indexing_topic_path}
            )
            logger.info(f"Created subscription {indexing_subscription_path}")
            break
        except AlreadyExists:
            logger.info(f"Subscription {indexing_subscription_path} already exists")
            break
        except PermissionDenied as e:
            logger.error(f"Permission denied creating subscription {indexing_subscription_path}: {e}")
            if attempt == 4:
                logger.critical(f"Cannot create subscription after {attempt + 1} attempts. Please create it manually with: gcloud pubsub subscriptions create {INDEXING_SUBSCRIPTION_NAME} --topic={INDEXING_TOPIC_NAME} --project={PROJECT_ID}")
                return
        except Exception as e:
            logger.error(f"Error creating subscription {indexing_subscription_path}: {e}")
            if attempt == 4:
                logger.critical(f"Failed to create subscription after {attempt + 1} attempts")
                return
            time.sleep(2 ** attempt)  # Exponential backoff
    
    # Verify subscription exists
    try:
        subscriber.get_subscription(request={"subscription": indexing_subscription_path})
        logger.info(f"Verified subscription {indexing_subscription_path} exists")
    except NotFound:
        logger.critical(f"Subscription {indexing_subscription_path} does not exist. Please create it manually with: gcloud pubsub subscriptions create {INDEXING_SUBSCRIPTION_NAME} --topic={INDEXING_TOPIC_NAME} --project={PROJECT_ID}")
        return
    except PermissionDenied as e:
        logger.warning(f"Permission denied verifying subscription {indexing_subscription_path}: {e}. Proceeding anyway, assuming it exists.")
    except Exception as e:
        logger.error(f"Error verifying subscription {indexing_subscription_path}: {e}")
        return
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    
    logger.info(f"Indexer node {NODE_ID} started")
    logger.info(f"Listening for indexing tasks on {indexing_subscription_path}...")
    
    while True:
        try:
            response = subscriber.pull(
                request={"subscription": indexing_subscription_path, "max_messages": 1},
                timeout=60
            )
            
            if not response.received_messages:
                logger.info("No messages received, waiting...")
                continue
            
            for message in response.received_messages:
                try:
                    task = json.loads(message.message.data.decode('utf-8'))
                    url = task.get("url")
                    gcs_path = task.get("gcs_path")
                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(f"Invalid message format: {e}")
                    subscriber.acknowledge(
                        request={"subscription": indexing_subscription_path, "ack_ids": [message.ack_id]}
                    )
                    continue
                
                logger.info(f"Received indexing task for {url} at {gcs_path}")
                
                if not url or not gcs_path:
                    logger.error(f"Missing url or gcs_path in task: {task}")
                    subscriber.acknowledge(
                        request={"subscription": indexing_subscription_path, "ack_ids": [message.ack_id]}
                    )
                    continue
                
                try:
                    process_content(url, gcs_path)
                    
                    # Notify task completion
                    publisher.publish(
                        task_complete_topic_path,
                        json.dumps({"node_id": NODE_ID, "url": url}).encode('utf-8')
                    )
                    logger.debug(f"Task completion notified for {url}")
                    
                    subscriber.acknowledge(
                        request={"subscription": indexing_subscription_path, "ack_ids": [message.ack_id]}
                    )
                    logger.info(f"Acknowledged message for {url}")
                except Exception as e:
                    logger.error(f"Error processing indexing task for {url}: {traceback.format_exc()}")
                    subscriber.modify_ack_deadline(
                        request={
                            "subscription": indexing_subscription_path,
                            "ack_ids": [message.ack_id],
                            "ack_deadline_seconds": 0,
                        }
                    )
        except NotFound as e:
            logger.error(f"Subscription {indexing_subscription_path} not found: {e}")
            break
        except PermissionDenied as e:
            logger.error(f"Permission denied pulling messages: {e}")
            break
        except Exception as e:
            logger.error(f"Error pulling messages: {traceback.format_exc()}")
            time.sleep(5)

if __name__ == '__main__':
    indexer_process()