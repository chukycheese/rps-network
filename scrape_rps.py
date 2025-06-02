import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import networkx as nx
from pyvis.network import Network
import time

def get_links_from_single_page(url):
    """
    Extracts all unique absolute links from a single URL.
    """
    print(f"Attempting to fetch links from: {url}")
    try:
        # Use a short timeout to prevent long hangs
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    links = set()

    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        absolute_url = urljoin(url, href)

        # Basic validation to ensure it's a valid HTTP/HTTPS URL with a network location
        parsed_url = urlparse(absolute_url)
        if parsed_url.scheme in ['http', 'https'] and parsed_url.netloc:
            # Remove fragment identifiers (e.g., #section) as they refer to the same page
            clean_url = urljoin(absolute_url, parsed_url.path)
            links.add(clean_url)
    return list(links)

def create_single_page_link_graph(target_url):
    """
    Creates a graph showing connections from a single target URL to all links on it.
    """
    graph = nx.DiGraph()
    graph.add_node(target_url) # Add the target URL as the central node

    print(f"Extracting links from: {target_url}")
    extracted_links = get_links_from_single_page(target_url)

    if not extracted_links:
        print(f"No links found or unable to fetch {target_url}.")
        return graph

    for link in extracted_links:
        graph.add_node(link) # Add each extracted link as a node
        graph.add_edge(target_url, link) # Create an edge from the target URL to the link

    return graph

if __name__ == "__main__":
    target_url = "http://www.rps.or.kr/theme/rps/index/partner_01.php"

    print("Starting graph creation...")
    link_graph = create_single_page_link_graph(target_url)

    print(f"\nGraph created:")
    print(f"Number of nodes: {link_graph.number_of_nodes()}")
    print(f"Number of edges: {link_graph.number_of_edges()}")

    if link_graph.number_of_nodes() > 0:
        # Create an interactive Pyvis visualization
        net = Network(notebook=True, cdn_resources='remote',
                      height="750px", width="100%",
                      bgcolor="#222222", font_color="white",
                      directed=True) # Ensure arrows for directed edges

        # Add nodes and edges to pyvis network
        # You can customize node colors/sizes based on whether it's the target URL
        for node in link_graph.nodes():
            if node == target_url:
                net.add_node(node, label="Target URL", color="red", size=20)
            else:
                net.add_node(node, label=node, color="lightblue", size=10)

        for edge in link_graph.edges():
            source, target = edge
            net.add_edge(source, target)

        # Generate and save the HTML file
        output_filename = "rps_partner_links_graph.html"
        net.show(output_filename)
        print(f"\nInteractive graph saved to {output_filename}")
        print(f"Open '{output_filename}' in your web browser to view the graph.")
    else:
        print("No graph generated as no nodes were found.")
