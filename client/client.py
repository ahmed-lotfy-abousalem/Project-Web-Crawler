import indexer.indexer as idx

def main():
    while True:
        keyword = input("Enter a keyword to search (or 'exit'): ")
        if keyword.lower() == 'exit':
            break
        results = idx.search(keyword)
        print(f"Found in URLs: {results}")

if __name__ == "__main__":
    main()
