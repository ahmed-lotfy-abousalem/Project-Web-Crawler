index = {}

def index_document(url, text):
    words = text.split()
    for word in words:
        word = word.lower()
        if word not in index:
            index[word] = []
        if url not in index[word]:
            index[word].append(url)

def search(keyword):
    keyword = keyword.lower()
    return index.get(keyword, [])

# Example: Just indexing a single document for testing
if __name__ == "__main__":
    index_document("https://example.com", "This is a sample page with example content.")
    print(search("example"))  # Should return ['https://example.com']
