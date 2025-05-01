import requests
from bs4 import BeautifulSoup
import redis
import json
import threading
import time
import uuid
import logging

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

# Redis connection
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Node identification
NODE_ID = str(uuid.uuid4())
MASTER_URL = "http://localhost:5000"

def send_heartbeat():
    """Continuously send heartbeats to master"""
    while True:
        try:
            response = requests.post(
                f"{MASTER_URL}/heartbeat",
                json={"node_id": NODE_ID},
                timeout=3
            )
            if response.status_code == 200:
                logger.debug("Heartbeat sent successfully")
            else:
                logger.warning(f"Heartbeat failed: HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"Heartbeat error: {str(e)}")
        time.sleep(3)  # Send every 3 seconds

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
    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()
    return ' '.join(soup.stripped_strings)

def send_result_to_master(crawled_data):
    try:
        r.lpush("crawled_results", json.dumps(crawled_data))
        logger.info(f"Sent results for {crawled_data['url']}")
    except Exception as e:
        logger.error(f"Failed to send results: {e}")

def notify_task_completion(url):
    try:
        response = requests.post(
            f"{MASTER_URL}/task_complete",
            json={"node_id": NODE_ID, "url": url},
            timeout=3
        )
        if response.status_code == 200:
            logger.debug(f"Task completion notified for {url}")
        else:
            logger.warning(f"Task completion notification failed: HTTP {response.status_code}")
    except Exception as e:
        logger.error(f"Task completion error: {str(e)}")

def crawl_task():
    logger.info(f"Starting crawler node {NODE_ID}")
    while True:
        try:
            # Get task from node-specific queue
            url = r.brpop(f"node:{NODE_ID}:tasks", timeout=0)[1].decode('utf-8')
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
                notify_task_completion(url)  # Even if failed, mark as complete
                logger.warning(f"Failed to process {url}")
                
        except Exception as e:
            logger.error(f"Crawler error: {e}")
            time.sleep(5)  # Prevent tight loop on errors

if __name__ == "__main__":
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    
    # Start crawling
    crawl_task()