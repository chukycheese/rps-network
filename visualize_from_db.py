import duckdb
import networkx as nx
from pyvis.network import Network
from urllib.parse import urlparse # For node label shortening

# --- Configuration (must match your previous DuckDB output) ---
DB_FILE = "website_full_network.duckdb" # Name of your DuckDB file
OUTPUT_HTML_GRAPH_FROM_DB = "index.html"
START_URL = "http://www.rps.or.kr/theme/rps/index/partner_01.php" # Needed for coloring the start node

def create_graph_from_duckdb(db_file, start_url):
    """
    Loads link data from DuckDB and constructs a NetworkX graph.
    """
    graph = nx.DiGraph()
    conn = None
    try:
        conn = duckdb.connect(database=db_file, read_only=True) # Open in read-only mode
        print(f"Connected to DuckDB database: {db_file}")

        # Fetch all edges from the website_links table
        # We also select crawl_depth if it exists, to preserve node depth for visualization
        try:
            # Check if crawl_depth column exists
            columns = conn.execute("PRAGMA table_info(website_links);").fetchall()
            if any(col[1] == 'crawl_depth' for col in columns):
                query = "SELECT source_url, target_url, crawl_depth FROM website_links;"
                data = conn.execute(query).fetchall()
                print("Including 'crawl_depth' from DuckDB.")
            else:
                query = "SELECT source_url, target_url FROM website_links;"
                data = conn.execute(query).fetchall()
                print("'crawl_depth' column not found, defaulting to depth 0 for all nodes.")

        except duckdb.Error as e:
            print(f"Error checking table schema or querying data: {e}")
            print("Assuming 'source_url' and 'target_url' are the only columns.")
            query = "SELECT source_url, target_url FROM website_links;"
            data = conn.execute(query).fetchall()


        print(f"Loaded {len(data)} link entries from DuckDB.")

        # Populate the NetworkX graph
        for row in data:
            source_url = row[0]
            target_url = row[1]
            crawl_depth = row[2] if len(row) > 2 else 0 # Default depth if not available

            # Add nodes (if they don't exist) with their respective depths
            if source_url not in graph:
                graph.add_node(source_url, depth=crawl_depth)
            if target_url not in graph:
                # When adding a target_url, its depth is usually source_depth + 1
                # However, if it's already added, we prefer the shallower depth
                # For simplicity here, we'll assign the target_depth from the edge for now,
                # but a full BFS from the DB might re-calculate optimal depths.
                # For visualization purposes, simply assign if not present.
                if target_url not in graph: # Ensure target node is added only once
                    graph.add_node(target_url, depth=crawl_depth + 1)
                else: # Update depth if we find a shallower path to it
                    current_target_depth = graph.nodes[target_url].get('depth', float('inf'))
                    if crawl_depth + 1 < current_target_depth:
                        graph.nodes[target_url]['depth'] = crawl_depth + 1


            # Add the edge
            graph.add_edge(source_url, target_url)

        # Ensure the start_url itself has depth 0
        if start_url in graph:
            graph.nodes[start_url]['depth'] = 0

        print(f"NetworkX graph created with {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges.")
        return graph

    except duckdb.Error as e:
        print(f"Error connecting to or querying DuckDB: {e}")
        return None
    finally:
        if conn:
            conn.close()

def visualize_graph(graph, start_url, output_html_file):
    """
    Generates an interactive Pyvis visualization from a NetworkX graph.
    """
    if not graph or graph.number_of_nodes() == 0:
        print("No graph data to visualize.")
        return

    net = Network(
        notebook=False,
        cdn_resources="remote",
        height="100%", width="100%",
        bgcolor="#222222", font_color="white",
        directed=True
    )

    for node_id, data in graph.nodes(data=True):
        depth = data.get('depth', 0) # Get depth from node data, default to 0

        # Define colors based on depth
        if node_id == start_url:
            color = "red"
        elif depth == 1:
            color = "#ADD8E6" # Light Blue
        elif depth == 2:
            color = "#87CEEB" # Sky Blue
        elif depth == 3:
            color = "#6A5ACD" # Slate Blue
        else:
            color = "lightblue" # Default for other depths

        # Create the clickable HTML title for the node
        node_title_html = f'<a href="{node_id}" target="_blank" style="color: white; text-decoration: underline;">{node_id}</a><br>Depth: {depth}'

        # Shorten the label displayed on the node for readability
        parsed_url = urlparse(node_id)
        domain_part = parsed_url.netloc
        path_part = parsed_url.path
        if len(path_part) > 20: # Arbitrary cut-off for path
            path_part = path_part[:15] + "..."
        display_label = f"{domain_part}{path_part}"
        if len(display_label) > 40: # Overall label length
            display_label = display_label[:37] + "..."

        net.add_node(node_id, label=display_label, color=color,
                     size=15 - (depth * 2), # Make deeper nodes slightly smaller
                     title=node_title_html)

    for source, target in graph.edges():
        net.add_edge(source, target)

    print(f"\nGenerating interactive graph to {output_html_file}...")
    # Generate HTML with CDN resources
    html = net.generate_html(notebook=False)
    with open(output_html_file, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Interactive graph saved to {output_html_file}")
    print(f"Open '{output_html_file}' in your web browser to view the graph.")

# --- Main Execution ---
if __name__ == "__main__":
    print(f"Attempting to load graph data from {DB_FILE}...")
    graph = create_graph_from_duckdb(DB_FILE, START_URL)

    if graph:
        print("Starting visualization...")
        visualize_graph(graph, START_URL, OUTPUT_HTML_GRAPH_FROM_DB)
    else:
        print(f"Could not load graph from {DB_FILE}. Make sure the DuckDB file exists and contains data.")
        print("You might need to run the scraping script first to generate the data.")
