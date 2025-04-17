class CrawlerNode:
    """
    Worker that pulls URL tasks, fetches HTML, extracts links/text, reports back.
    """
    def __init__(self, node_id: str, config: dict):
        self.node_id = node_id
        self.config = config
        # e.g. self.queue = init_queue(config['queue'])
        #      self.storage = init_storage(config['storage'])

    def run(self):
        """Main loop: fetch tasks, crawl, push results, send heartbeats."""
        raise NotImplementedError

    def fetch_page(self, url: str) -> str:
        """Download HTML and return raw text."""
        raise NotImplementedError

    def parse_links(self, html: str) -> list[str]:
        """Extract and return list of hyperlinks."""
        raise NotImplementedError


if __name__ == '__main__':
    cfg = {}
    crawler = CrawlerNode(node_id="crawler-01", config=cfg)
    crawler.run()
