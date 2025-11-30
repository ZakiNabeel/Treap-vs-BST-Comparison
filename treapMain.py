%%writefile treapMain.py
import time
import matplotlib.pyplot as plt
import os
import sys

from src.Treap.treap import Treap
from src.Utility.utils import stream_reddit_dataset, get_structural_metrics

DATASET_PATH = "/kaggle/input/the-pushshift-reddit-dataset-submissions/RC_2019-04.zst"

def main():
    # --- COMMAND LINE ARGUMENT HANDLING ---
    limit_arg = 50000 
    if len(sys.argv) > 1:
        try:
            val = int(sys.argv[1])
            if val > 0: limit_arg = val
        except: pass

    print(f"=== SINGLE TREAP IMPLEMENTATION ===")
    print(f"Processing {limit_arg} nodes...")
    
    treap = Treap()
    start_time = time.perf_counter()
    count = 0
    
    if not os.path.exists(DATASET_PATH): return

    for pid, ts, score in stream_reddit_dataset(DATASET_PATH, limit=limit_arg):
        treap.addPost(pid, ts, score)
        count += 1
        if count % 10000 == 0: print(f"Inserted {count}...")
            
    total_time = time.perf_counter() - start_time
    avg_insertion = total_time / count if count > 0 else 0
    
    print("\n--- PERFORMANCE METRICS ---")
    print(f"Total Time: {total_time:.4f} sec")
    print(f"Total Rotations: {treap.rotations_count}")
    
    print("Calculating Metrics...")
    height, total_bf, node_count = get_structural_metrics(treap.root)
    avg_bf = total_bf / node_count if node_count > 0 else 0
    
    print(f"Tree Height: {height}")
    print(f"Avg Balance Factor: {avg_bf:.2f}")

    # Generate Graph
    metrics = ['Avg Insert (x1k ms)', 'Height', 'Avg Bal. Factor']
    values = [avg_insertion * 1000, height, avg_bf] 
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(metrics, values, color=['darkblue', 'blue', 'skyblue'])
    plt.title(f"Treap Metrics (N={count})")
    for bar in bars:
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), round(bar.get_height(), 2), ha='center', va='bottom')
    plt.savefig('treap_metrics_local.png')
    print("Graph saved: treap_metrics_local.png")

if __name__ == "__main__":
    main()
