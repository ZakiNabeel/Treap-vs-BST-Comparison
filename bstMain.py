import time
import matplotlib.pyplot as plt
import os
import sys
import importlib.util

# Increase recursion for the BST "Stick"
sys.setrecursionlimit(200000)

# Import Fix for Folder with Spaces
try:
    bst_path = os.path.join("src", "Binary Search Tree", "bst.py")
    spec = importlib.util.spec_from_file_location("bst_module", bst_path)
    bst_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bst_module)
    BST = bst_module.BST
except FileNotFoundError:
    # Fallback to standard if renamed
    from src.BinarySearchTree.bst import BST

from src.Utility.utils import stream_reddit_dataset, get_structural_metrics

DATASET_PATH = "/kaggle/input/the-pushshift-reddit-dataset-submissions/RC_2019-04.zst"

def main():
    # --- COMMAND LINE ARGUMENT HANDLING ---
    limit_arg = 50000 # Default safe limit for BST
    if len(sys.argv) > 1:
        try:
            val = int(sys.argv[1])
            if val > 0: limit_arg = val
        except: pass
    
    print(f"=== BST IMPLEMENTATION (Control Group) ===")
    print(f"Processing {limit_arg} nodes...")
    print("Warning: BST degrades to O(N) height on sorted data.")
    
    bst = BST()
    start_time = time.perf_counter()
    count = 0
    
    # Stream Data
    if not os.path.exists(DATASET_PATH):
        print("Dataset not found.")
        return

    for pid, ts, score in stream_reddit_dataset(DATASET_PATH, limit=limit_arg):
        bst.addPost(pid, ts, score)
        count += 1
        if count % 5000 == 0: print(f"Inserted {count}...")
            
    total_time = time.perf_counter() - start_time
    avg_insertion = total_time / count if count > 0 else 0
    
    print("\n--- PERFORMANCE METRICS ---")
    print(f"Total Time: {total_time:.4f} sec")
    print(f"Avg Insertion Time: {avg_insertion:.8f} sec")
    
    # Structural Metrics
    print("Calculating Metrics (O(N))...")
    height, total_bf, node_count = get_structural_metrics(bst.root)
    avg_bf = total_bf / node_count if node_count > 0 else 0
    
    print(f"Tree Height: {height}")
    print(f"Avg Balance Factor: {avg_bf:.2f}")
    
    # Generate Graph
    metrics = ['Avg Insert (x1k ms)', 'Height', 'Avg Bal. Factor']
    values = [avg_insertion * 1000, height, avg_bf] 
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(metrics, values, color=['darkred', 'red', 'salmon'])
    plt.title(f"BST Metrics (N={count})")
    for bar in bars:
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), round(bar.get_height(), 2), ha='center', va='bottom')
    plt.savefig('bst_metrics_local.png')
    print("Graph saved: bst_metrics_local.png")

if __name__ == "__main__":
    main()
