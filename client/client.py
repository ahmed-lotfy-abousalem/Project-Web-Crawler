import requests

def check_status():
    response = requests.get("http://localhost:5000/status")
    print("Active Nodes:", response.json()["active_nodes"])
    print("Tasks in Queue:", response.json()["tasks_in_queue"])

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
