import requests
from bs4 import BeautifulSoup
import redis
import json

# Connect to Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

def fetch_page(url):
    try:
        response = requests.get(url, timeout=5)
        return response.text
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
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
    return soup.get_text()

def send_result_to_master(crawled_data):
    # Send the data back to Master node via Redis
    r.lpush("crawled_results", json.dumps(crawled_data))

def crawl_task():
    while True:
        # Pull a URL from the queue
         url = r.brpop("url_queue", timeout=0)  # Block until we get a URL
         if url:
            url = url[1].decode('utf-8')
            print(f"Crawling URL: {url}")
            page = fetch_page(url)
            if page:
                text = extract_text(page)
                links = extract_links(page, url)
                data = {'url': url, 'text': text, 'links': links}
                send_result_to_master(data)
                print(f"Fetched links: {links}")

if __name__ == "__main__":
    crawl_task()
