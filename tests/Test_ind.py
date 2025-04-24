from mpi4py import MPI
import time

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:  # Test Crawler
    test_pages = [
        (1, "http://example.com", "Example Domain", 
         "This domain is for use in illustrative examples in documents."),
        (2, "http://example.edu", "Education Example",
         "Sample content about distributed systems education.")
    ]
    
    for task in test_pages:
        comm.send(task, dest=1, tag=2)
        print(f"Crawler sent: {task[1]}")
        time.sleep(1)
    
    # Test search after indexing
    time.sleep(2)
    comm.send("distributed systems", dest=1, tag=3)
    results = comm.recv(source=1, tag=4)
    print("\nSearch Results:")
    for res in results:
        print(f"- {res['title']}: {res['snippet']}")

elif rank == 1:  # Indexer (will be run separately)
    pass  