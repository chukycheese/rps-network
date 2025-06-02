import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import networkx as nx
from pyvis.network import Network
import duckdb # Import duckdb
import os # For checking if db file exists

def get_links_from_single_page(url):
    """
    Extracts all unique absolute links from a single URL.
    """
    print(f"Attempting to fetch links from: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    links = set()

    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        absolute_url = urljoin(url, href)

        parsed_url = urlparse(absolute_url)
        if parsed_url.scheme in ['http', 'https'] and parsed_url.netloc:
            clean_url = urljoin(absolute_url, parsed_url.path)
            links.add(clean_url)
    return list(links)

def create_single_page_link_graph(target_url, conn):
    """
    Creates a graph showing connections from a single target URL to all links on it,
    and stores the data in the provided DuckDB connection.
    """
    graph = nx.DiGraph()
    graph.add_node(target_url)

    print(f"Extracting links from: {target_url}")
    extracted_links = get_links_from_single_page(target_url)

    if not extracted_links:
        print(f"No links found or unable to fetch {target_url}.")
        return graph

    # Prepare data for insertion
    data_to_insert = []
    for link in extracted_links:
        graph.add_node(link)
        graph.add_edge(target_url, link)
        data_to_insert.append((target_url, link)) # (source_url, target_url)

    # Insert data into DuckDB
    try:
        # Use executemany for efficient insertion of multiple rows
        conn.executemany("INSERT INTO website_links (source_url, target_url) VALUES (?, ?)", data_to_insert)
        print(f"Successfully inserted {len(data_to_insert)} links into DuckDB.")
    except duckdb.Error as e:
        print(f"Error inserting data into DuckDB: {e}")

    return graph

if __name__ == "__main__":
    target_url = "http://www.rps.or.kr/theme/rps/index/partner_01.php"
    db_file = "website_links.duckdb" # Name for your DuckDB file

    # --- 1. Connect to DuckDB ---
    # If the file exists, it will connect to it. If not, it will create it.
    conn = duckdb.connect(database=db_file)
    print(f"Connected to DuckDB database: {db_file}")

    # --- 2. Create Table if not exists ---
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS website_links (
                source_url VARCHAR,
                target_url VARCHAR
            );
        """)
        print("Table 'website_links' ensured to exist.")
    except duckdb.Error as e:
        print(f"Error creating table in DuckDB: {e}")
        conn.close()
        exit()

    print("Starting graph creation and data storage...")
    link_graph = create_single_page_link_graph(target_url, conn)

    print(f"\nGraph created:")
    print(f"Number of nodes: {link_graph.number_of_nodes()}")
    print(f"Number of edges: {link_graph.number_of_edges()}")

    # --- 3. Query Data from DuckDB (Optional) ---
    print("\n--- Data in DuckDB (top 5 rows) ---")
    try:
        result = conn.execute("SELECT source_url, target_url FROM website_links LIMIT 5").fetchall()
        for row in result:
            print(f"Source: {row[0]}, Target: {row[1]}")
    except duckdb.Error as e:
        print(f"Error querying data from DuckDB: {e}")


    # --- 4. Close the DuckDB connection ---
    conn.close()
    print(f"DuckDB connection to {db_file} closed.")


    # --- 5. Generate NetworkX visualization (same as before) ---
    if link_graph.number_of_nodes() > 0:
        net = Network(notebook=True, cdn_resources='remote',
                      height="750px", width="100%",
                      bgcolor="#222222", font_color="white",
                      directed=True)

        for node in link_graph.nodes():
            if node == target_url:
                net.add_node(node, label="Target URL", color="red", size=20)
            else:
                net.add_node(node, label=node, color="lightblue", size=10)

        for edge in link_graph.edges():
            source, target = edge
            net.add_edge(source, target)

        output_filename = "rps_partner_links_graph_with_duckdb.html"
        net.show(output_filename)
        print(f"\nInteractive graph saved to {output_filename}")
        print(f"Open '{output_filename}' in your web browser to view the graph.")
    else:
        print("No graph generated as no nodes were found.")
