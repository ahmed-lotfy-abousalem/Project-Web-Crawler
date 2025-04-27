import sqlite3

# Connect to the SQLite database
def connect_db():
    conn = sqlite3.connect('crawler_data.db')
    return conn

# Search for a keyword in the crawled data
def search_keyword(keyword):
    conn = connect_db()
    c = conn.cursor()
    
    # Search for the keyword in the 'text' field of crawled data
    query = "SELECT url, text FROM crawled_data WHERE text LIKE ?"
    c.execute(query, ('%' + keyword + '%',))
    
    # Fetch all matching results
    results = c.fetchall()
    
    conn.close()
    
    # If results are found, return them
    if results:
        return results
    else:
        return None

# Example usage:
if __name__ == "__main__":
    # Test the keyword search functionality
    keyword = "example"  # Replace with the keyword you want to search for
    search_results = search_keyword(keyword)
    
    if search_results:
        print(f"Found {len(search_results)} result(s) for keyword '{keyword}':")
        for result in search_results:
            print(f"URL: {result[0]}, Content: {result[1][:100]}...")  # Display the first 100 chars of content
    else:
        print(f"No results found for keyword '{keyword}'")
