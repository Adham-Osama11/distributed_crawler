# src/master_node.py

import threading
import time
import uuid
import json
import logging

import boto3

class MasterNode:
    """
    Master scheduler: enqueues crawl tasks to SQS, 
    tracks crawler heartbeats, and manages crawler lifecycle.
    """

    def __init__(self, task_queue_url: str, heartbeat_timeout: int = 90):
        # SQS client (credentials & region via env or IAM role)
        self.sqs = boto3.client('sqs')
        self.task_queue_url = task_queue_url

        # Heartbeat & lifecycle tracking
        self.heartbeat_timeout = heartbeat_timeout
        self.active_crawlers: dict[str, float] = {}  # node_id -> last_heartbeat

        # Start background monitor
        self._monitor_thread = threading.Thread(
            target=self._monitor_heartbeats, daemon=True
        )
        self._monitor_thread.start()

        logging.basicConfig(level=logging.INFO)

    # ——————————————
    # 1. Divide seeds & enqueue
    # ——————————————
    def enqueue_seeds(self, seeds: list[str], max_depth: int):
        """
        Split each seed URL into a task and send to SQS.
        """
        for url in seeds:
            task = {
                "task_id": str(uuid.uuid4()),
                "url": url,
                "depth_left": max_depth,
                "retry_count": 0
            }
            msg = json.dumps(task)
            resp = self.sqs.send_message(
                QueueUrl=self.task_queue_url,
                MessageBody=msg
            )
            logging.info(f"[Master] Enqueued task {task['task_id']} for {url} "
                            f"(msgId={resp.get('MessageId')})")

    # ——————————————
    # 2. Task assignment
    # ——————————————
    # Crawlers will pull tasks directly from the SQS queue, so 'assignment'
    # here is simply the act of enqueueing. If you wanted to push to
    # specific nodes, you could use message attributes or dedicated queues.

    # ——————————————
    # 3. Lifecycle Management
    # ——————————————
    def register_crawler(self, node_id: str):
        """
        Called when a crawler starts up (or re-registers).
        """
        now = time.time()
        self.active_crawlers[node_id] = now
        logging.info(f"[Master] Registered crawler {node_id} at {now}")

    def deregister_crawler(self, node_id: str):
        """
        Called when a crawler cleanly shuts down or is considered dead.
        """
        if node_id in self.active_crawlers:
            del self.active_crawlers[node_id]
            logging.info(f"[Master] Deregistered crawler {node_id}")

    def receive_heartbeat(self, node_id: str):
        """
        Called by your heartbeat API (or message) whenever a crawler pings.
        """
        now = time.time()
        if node_id not in self.active_crawlers:
            logging.info(f"[Master] Heartbeat from unknown crawler {node_id}; registering.")
        self.active_crawlers[node_id] = now
        logging.debug(f"[Master] Heartbeat updated for {node_id} → {now}")

    def _monitor_heartbeats(self):
        """
        Background thread: periodically scans for stale heartbeats.
        """
        check_interval = max(1, self.heartbeat_timeout // 3)
        while True:
            now = time.time()
            stale = []
            for node_id, last in list(self.active_crawlers.items()):
                if now - last > self.heartbeat_timeout:
                    stale.append(node_id)

            for node_id in stale:
                logging.warning(
                    f"[Master] No heartbeat from {node_id} for {(now-last):.0f}s → deregistering"
                )
                self.deregister_crawler(node_id)

            time.sleep(check_interval)

    # ——————————————
    # 4. Master run loop
    # ——————————————
    def start(self, seeds: list[str], max_depth: int):
        """
        Kick off the crawl and then simply keep the master alive
        to monitor heartbeats (or optionally serve an API).
        """
        logging.info("[Master] Starting crawl...")
        self.enqueue_seeds(seeds, max_depth)
        logging.info("[Master] Seeds enqueued; entering monitoring loop.")
        # In a real system you might also start an HTTP server here.
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("[Master] Shutting down.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-url", required=True,
                        help="SQS URL for crawl-task queue")
    parser.add_argument("--heartbeat-timeout", type=int, default=90,
                        help="Seconds before marking crawler dead")
    parser.add_argument("--max-depth", type=int, default=2,
                        help="Max link-hops from seeds")
    parser.add_argument("seeds", nargs="+",
                        help="One or more seed URLs")
    args = parser.parse_args()

    master = MasterNode(
        task_queue_url=args.queue_url,
        heartbeat_timeout=args.heartbeat_timeout
    )
    master.start(seeds=args.seeds, max_depth=args.max_depth)
