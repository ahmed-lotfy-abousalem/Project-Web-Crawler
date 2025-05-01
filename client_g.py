import requests
from indexer import search_index

def check_status():
    response = requests.get("http://34.173.15.32:5000/status")  # Replace with master VM's external IP
    print("Active Nodes:", response.json()["active_nodes"])
    print("Pending Tasks:", response.json()["pending_tasks"])
    print("Completed Tasks:", response.json()["completed_tasks"])

def search(query):
    results = search_index(query)
    for url, title, score in results:
        print(f"URL: {url}, Title: {title}, Score: {score}")

if __name__ == "__main__":
    while True:
        cmd = input("Enter command (search/status/exit): ")
        if cmd == "search":
            query = input("Enter search query: ")
            search(query)
        elif cmd == "status":
            check_status()
        elif cmd == "exit":
            break