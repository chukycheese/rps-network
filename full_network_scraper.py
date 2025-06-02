import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import networkx as nx
from pyvis.network import Network
import duckdb
import time
import os
from collections import deque

# --- Configuration ---
START_URL = "http://www.rps.or.kr/theme/rps/index/partner_01.php"
MAX_CRAWL_DEPTH = 2  # How many "hops" from the starting URL
DB_FILE = "website_full_network.duckdb" # New DuckDB file for the full network
OUTPUT_HTML_GRAPH = "rps_full_network_graph.html"
REQUEST_TIMEOUT = 10 # seconds
CRAWL_DELAY = 1 # seconds between requests to be polite

class CrawlManager:
    def __init__(self, start_url, max_depth, db_file):
        self.start_url = start_url
        self.max_depth = max_depth
        self.visited_urls = set()
        self.urls_to_visit = deque([(start_url, 0)]) # (url, current_depth)
        self.graph = nx.DiGraph()
        self.db_file = db_file
        self.conn = None
        self.base_domain = urlparse(start_url).netloc # To stay within the original domain

        self._setup_db()

    def _setup_db(self):
        """Connects to DuckDB and creates the table."""
        try:
            self.conn = duckdb.connect(database=self.db_file)
            print(f"Connected to DuckDB database: {self.db_file}")

            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS website_links (
                    source_url VARCHAR,
                    target_url VARCHAR,
                    crawl_depth INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("Table 'website_links' ensured to exist.")
        except duckdb.Error as e:
            print(f"Error setting up DuckDB: {e}")
            if self.conn:
                self.conn.close()
            exit()

    def _get_links_from_page(self, url):
        """Fetches a page and extracts unique absolute links."""
        print(f"Fetching: {url}")
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
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
            # Filter for valid HTTP/HTTPS and potentially same-domain links
            if parsed_url.scheme in ['http', 'https'] and parsed_url.netloc:
                clean_url = urljoin(absolute_url, parsed_url.path)
                # Uncomment the following line to restrict crawling to the same domain
                # if parsed_url.netloc == self.base_domain:
                links.add(clean_url)
        return list(links)

    def crawl(self):
        """Performs the web crawl."""
        while self.urls_to_visit:
            current_url, current_depth = self.urls_to_visit.popleft()

            if current_url in self.visited_urls or current_depth > self.max_depth:
                print(f"Skipping {current_url} (already visited or too deep)")
                continue

            self.visited_urls.add(current_url)
            self.graph.add_node(current_url, depth=current_depth)
            print(f"Crawling {current_url} at depth {current_depth}")

            time.sleep(CRAWL_DELAY) # Polite delay

            found_links = self._get_links_from_page(current_url)
            links_to_insert = []

            for link in found_links:
                self.graph.add_edge(current_url, link)
                links_to_insert.append((current_url, link, current_depth))

                if link not in self.visited_urls:
                    self.urls_to_visit.append((link, current_depth + 1))

            if links_to_insert:
                try:
                    self.conn.executemany(
                        "INSERT INTO website_links (source_url, target_url, crawl_depth) VALUES (?, ?, ?)",
                        links_to_insert
                    )
                    print(f"  Inserted {len(links_to_insert)} edges into DuckDB from {current_url}")
                except duckdb.Error as e:
                    print(f"  Error inserting data for {current_url}: {e}")

        print("\n--- Crawl Finished ---")
        print(f"Total nodes in graph: {self.graph.number_of_nodes()}")
        print(f"Total edges in graph: {self.graph.number_of_edges()}")

    def close_db(self):
        """Closes the DuckDB connection."""
        if self.conn:
            self.conn.close()
            print(f"DuckDB connection to {self.db_file} closed.")

    def get_graph(self):
        return self.graph

    def query_db_sample(self, limit=5):
        """Queries and prints a sample of data from DuckDB."""
        print(f"\n--- Sample Data from {self.db_file} (top {limit} rows) ---")
        try:
            result = self.conn.execute(f"SELECT source_url, target_url, crawl_depth FROM website_links LIMIT {limit}").fetchall()
            for row in result:
                print(f"Source: {row[0]}, Target: {row[1]}, Depth: {row[2]}")
        except duckdb.Error as e:
            print(f"Error querying data from DuckDB: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Removed existing DuckDB file: {DB_FILE}")

    crawler = CrawlManager(START_URL, MAX_CRAWL_DEPTH, DB_FILE)
    crawler.crawl()
    crawler_graph = crawler.get_graph()

    crawler.query_db_sample()
    crawler.close_db()

    # --- Visualization ---
    if crawler_graph.number_of_nodes() > 0:
        net = Network(notebook=True, cdn_resources='remote',
                      height="750px", width="100%",
                      bgcolor="#222222", font_color="white",
                      directed=True)

        for node, data in crawler_graph.nodes(data=True):
            depth = data.get('depth', 0)
            
            # Define colors based on depth (adjust as desired)
            if node == START_URL:
                color = "red"
            elif depth == 1:
                color = "#ADD8E6" # Light Blue
            elif depth == 2:
                color = "#87CEEB" # Sky Blue
            else:
                color = "lightblue"

            # Create the clickable HTML title for the node
            # The 'target="_blank"' makes the link open in a new tab
            # The 'title' attribute of the node itself is what pyvis displays on hover
            node_title_html = f'<a href="{node}" target="_blank" style="color: white; text-decoration: underline;">{node}</a><br>Depth: {depth}'
            
            # Shorten the label displayed on the node for readability in the graph
            # You might want a more sophisticated shortening logic for very long URLs
            display_label = urlparse(node).netloc + urlparse(node).path[:15] + "..." if len(node) > 30 else node


            net.add_node(node, label=display_label, color=color, size=15 - (depth * 2), title=node_title_html)

        for source, target in crawler_graph.edges():
            net.add_edge(source, target)

        print(f"\nGenerating interactive graph to {OUTPUT_HTML_GRAPH}...")
        net.show(OUTPUT_HTML_GRAPH)
        print(f"Interactive graph saved to {OUTPUT_HTML_GRAPH}")
        print(f"Open '{OUTPUT_HTML_GRAPH}' in your web browser to view the full network.")
    else:
        print("No graph generated as no nodes were found during crawling.")
