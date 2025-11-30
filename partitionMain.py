import time
import os
import sys

# Increase recursion limit because merging massive trees can be deep.
# Default is 1000; we need more for processing millions of nodes safely.
sys.setrecursionlimit(200000)

from src.Treap.treap import Treap
from src.Utility.utils import stream_reddit_dataset, get_structural_metrics

# CONFIGURATION
DATASET_PATH = "/kaggle/input/the-pushshift-reddit-dataset-submissions/RC_2019-04.zst"
PARTITION_SIZE = 100000  # Process 100k nodes at a time (RAM efficient)

def main():
    print(f"=== n-TREAPS PARTITIONING STRATEGY ===")
    print(f"Strategy: Divide (Build Partitions) & Conquer (Union Merge)")
    
    # --- COMMAND LINE ARGUMENT HANDLING ---
    limit_arg = None # Default: None (Entire Dataset)
    if len(sys.argv) > 1:
        try:
            val = int(sys.argv[1])
            if val > 0:
                limit_arg = val
                print(f"Goal: Process {limit_arg} nodes.")
            else:
                print(f"Goal: Process ENTIRE DATASET (Limit=0)")
        except ValueError:
            print("Invalid argument. Using default (Entire Dataset).")
    else:
        print("No limit argument provided. Processing ENTIRE DATASET.")
    
    # The Master Treap starts empty
    master_treap = Treap()
    start_total = time.perf_counter()
    
    # Stream Generator
    dataset_stream = stream_reddit_dataset(DATASET_PATH, limit=limit_arg)
    
    total_processed = 0
    partition_count = 0
    
    try:
        while True:
            partition_count += 1
            chunk_nodes = []
            
            # 1. READ CHUNK (Linear Scan)
            try:
                for _ in range(PARTITION_SIZE):
                    chunk_nodes.append(next(dataset_stream))
            except StopIteration:
                pass # Stream ended
            
            if not chunk_nodes:
                print(">>> End of dataset stream reached.")
                break
                
            # 2. BUILD INDEPENDENT TREAP (Divide)
            t0 = time.perf_counter()
            temp_treap = Treap()
            for pid, ts, score in chunk_nodes:
                temp_treap.addPost(pid, ts, score)
            t_build = time.perf_counter() - t0
            
            # 3. MERGE INTO MASTER (Conquer via Union)
            t0 = time.perf_counter()
            master_treap.union(temp_treap)
            t_merge = time.perf_counter() - t0
            
            # 4. MEMORY CLEANUP
            del temp_treap
            del chunk_nodes
            
            total_processed += PARTITION_SIZE
            print(f"Partition {partition_count}: Built {t_build:.2f}s | Merged {t_merge:.2f}s | Total: {total_processed}")
            
            if total_processed % 1000000 == 0:
                print(f"*** MILESTONE: {total_processed/1000000:.1f}M Nodes ***")

    except KeyboardInterrupt:
        print("\n!!! User stopped execution. Finalizing... !!!")

    # --- FINAL METRICS ---
    final_time = time.perf_counter() - start_total
    print("\n" + "="*40)
    print("=== FINAL RESULTS ===")
    print("="*40)
    print(f"Total Nodes: {total_processed}")
    print(f"Total Time:  {final_time:.4f}s")
    
    print("\nCalculating Structural Metrics (Height & Balance)...")
    # Uses O(N) optimized calculation
    height, total_bf, count = get_structural_metrics(master_treap.root)
    avg_bf = total_bf / count if count > 0 else 0
    
    print(f"Final Tree Height:     {height}")
    print(f"Avg Balance Factor:    {avg_bf:.2f}")
    
    if height < 300:
        print("\n[SUCCESS] Treap maintained logarithmic height.")
    else:
        print("\n[NOTE] Height is large. Verify score randomization.")

if __name__ == "__main__":
    main()
