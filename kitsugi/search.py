import sqlite3
import json
import sys

def run_search(db_conn, query_string):
    """
    Performs a full-text search on the database and prints the results
    as a single JSON object to standard output.
    
    Args:
        db_conn: An active sqlite3 database connection.
        query_string: The FTS5 search query string.
    """
    cursor = db_conn.cursor()
    # Print progress to stderr so it doesn't corrupt the JSON output
    print(f"Searching for snippets matching: '{query_string}'", file=sys.stderr)

    # The FTS5 query joining the search index with the location index
    query = """
        SELECT
            s.data,
            i.location
        FROM
            data_search_idx s
        JOIN
            hash_index i ON s.hash = i.hash
        WHERE
            data_search_idx MATCH ?
        ORDER BY
            i.location;
    """
    
    try:
        cursor.execute(query, (query_string,))
        results = cursor.fetchall()
    except sqlite3.OperationalError as e:
        # If the query itself is invalid, output a JSON error object
        error_output = {
            "error": "Error during FTS5 search query.",
            "query": query_string,
            "details": str(e),
            "suggestion": "Check your query syntax. Use quotes for phrases and operators like AND, OR, NOT."
        }
        print(json.dumps(error_output, indent=2))
        return

    # Group the flat list of results into a structured dictionary
    matches_by_location = {}
    for data, location in results:
        if location not in matches_by_location:
            matches_by_location[location] = []
        # json.loads converts the stored string back into a Python type
        # (e.g., the string '"hello"' becomes the Python string 'hello')
        matches_by_location[location].append(json.loads(data))
        
    # Prepare the final JSON object for output
    final_output = {
        'search_query': query_string,
        'total_matches': len(results),
        'matches_by_location': matches_by_location
    }

    # Print the final, complete JSON object to standard output
    print(json.dumps(final_output, indent=2))

