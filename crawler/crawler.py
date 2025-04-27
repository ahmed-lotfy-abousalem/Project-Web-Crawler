import requests
from bs4 import BeautifulSoup
import json
import socket

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
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('master_ip', 9000))  # Replace master_ip
    s.sendall(json.dumps(crawled_data).encode())
    s.close()

def crawl_task(url):
    page = fetch_page(url)
    if page:
        text = extract_text(page)
        links = extract_links(page, url)
        data = {'url': url, 'text': text, 'links': links}
        send_result_to_master(data)
