import time
import os
import sys

# Increase recursion limit because merging large trees (Union) can be deep.
# Default Python limit is 1000, which is insufficient for deep merges.
sys.setrecursionlimit(200000)

from src.Treap.treap import Treap
from src.Utility.utils import stream_reddit_dataset, get_structural_metrics

# CONFIGURATION
# Using the full 15GB (compressed) / 180GB (raw) dataset
DATASET_PATH = "/kaggle/input/the-pushshift-reddit-dataset-submissions/RC_2019-04.zst"

# [cite_start]Chunk size for the "n-Treaps" strategy[cite: 36].
# 100,000 nodes is a sweet spot: fits easily in RAM but large enough to be efficient.
PARTITION_SIZE = 100000


def main():
    print(f"=== n-TREAPS FULL DATASET RUN ===")
    print(f"Strategy: Partitioning via Multiple Treaps (Divide & Conquer)")
    print(f"Goal: Process unlimited nodes by merging partitions.")
    print(f"Chunk Size: {PARTITION_SIZE}\n")

    # The Master Treap starts empty. It will accumulate all data.
    master_treap = Treap()
    start_total = time.perf_counter()

    # [cite_start]Initialize the generator to stream data line-by-line [cite: 32]
    # This avoids loading 180GB into RAM at once.
    dataset_stream = stream_reddit_dataset(DATASET_PATH, limit=None)

    total_processed = 0
    partition_count = 0

    try:
        # Continuous loop until dataset is exhausted
        while True:
            partition_count += 1
            chunk_nodes = []

            # 1. READ A CHUNK (Linear Stream)
            # Fetch the next 'PARTITION_SIZE' items from the file
            try:
                for _ in range(PARTITION_SIZE):
                    chunk_nodes.append(next(dataset_stream))
            except StopIteration:
                pass  # Stream finished

            # If the chunk is empty, we have processed the entire file.
            if not chunk_nodes:
                print(">>> End of dataset stream reached.")
                break

            # [cite_start]2. BUILD INDEPENDENT TREAP (The "Divide" Step) [cite: 37]
            # Construct a small local Treap for this partition.
            t0 = time.perf_counter()
            temp_treap = Treap()
            for pid, ts, score in chunk_nodes:
                temp_treap.addPost(pid, ts, score)
            t_build = time.perf_counter() - t0

            # [cite_start]3. MERGE INTO MASTER (The "Conquer" Step) [cite: 37, 52]
            # Use the 'Union' operation to merge the partition into the main tree.
            # This maintains the Heap Priority logic across the entire dataset.
            t0 = time.perf_counter()
            master_treap.union(temp_treap)
            t_merge = time.perf_counter() - t0

            # 4. MEMORY CLEANUP
            # Critical Step: Delete the temporary partition to free RAM.
            # This ensures we only hold the Master Tree + 1 Chunk in memory.
            del temp_treap
            del chunk_nodes

            total_processed += PARTITION_SIZE

            # Log progress for every partition
            print(
                f"Partition {partition_count}: Built {t_build:.2f}s | Merged {t_merge:.2f}s | Total: {total_processed}")

            # Print Milestone every 1 Million Nodes to track scalability
            if total_processed % 1000000 == 0:
                print(f"*** MILESTONE: {total_processed / 1000000:.1f}M Nodes Processed ***")

    except KeyboardInterrupt:
        print("\n!!! User stopped execution. Finalizing metrics... !!!")

    final_time = time.perf_counter() - start_total
    print(f"\nTotal Nodes Processed: {total_processed}")
    print(f"Total Execution Time:  {final_time:.4f}s")

    # [cite_start]5. FINAL STRUCTURAL ANALYSIS [cite: 10]
    print("\nCalculating Final Metrics (Height & Balance Factor)...")

    # Use the O(N) optimized metric function to avoid recursion limits/timeouts
    height, total_bf, count = get_structural_metrics(master_treap.root)
    avg_bf = total_bf / count if count > 0 else 0

    print(f"Final Tree Height:     {height}")
    print(f"Avg Balance Factor:    {avg_bf:.2f}")

    if height < 300:
        print("\nSUCCESS: The Treap maintained logarithmic height despite massive size!")


if __name__ == "__main__":
    main()