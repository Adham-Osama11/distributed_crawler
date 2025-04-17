class IndexerNode:
    """
    Worker that consumes processed text, builds/merges index shards, serves search queries.
    """
    def __init__(self, node_id: str, config: dict):
        self.node_id = node_id
        self.config = config
        # e.g. self.index = init_index(config['index_path'])

    def run(self):
        """Main loop: watch for new text, update index."""
        raise NotImplementedError

    def index_text(self, page_id: str, text: str):
        """Tokenize & insert into inverted index."""
        raise NotImplementedError

    def search(self, query: str, top_k: int = 10) -> list[tuple[str, float]]:
        """Return list of (url, score)."""
        raise NotImplementedError


if __name__ == '__main__':
    cfg = {}
    indexer = IndexerNode(node_id="indexer-01", config=cfg)
    indexer.run()
