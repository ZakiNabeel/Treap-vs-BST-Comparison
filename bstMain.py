import time
import matplotlib.pyplot as plt
import os
import sys
import importlib.util

# FIX RECURSION LIMIT
sys.setrecursionlimit(100000)

# FIX FOLDER NAME
try:
    bst_path = os.path.join("src", "Binary Search Tree", "bst.py")
    spec = importlib.util.spec_from_file_location("bst_module", bst_path)
    bst_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bst_module)
    BST = bst_module.BST
except FileNotFoundError:
    print("CRITICAL ERROR: Could not find bst.py")
    sys.exit(1)

# IMPORT NEW OPTIMIZED FUNCTION
from src.Utility.utils import stream_reddit_dataset, get_structural_metrics

# CONFIG
DATASET_PATH = "/kaggle/input/the-pushshift-reddit-dataset-submissions/RC_2019-04.zst"
LIMIT = 50000

def main():
    print(f"=== BST IMPLEMENTATION (Processing {LIMIT} nodes) ===")
    bst = BST()
    
    # 1. INSERTION
    print(f"Streaming data...")
    start_time = time.perf_counter()
    count = 0
    for pid, ts, score in stream_reddit_dataset(DATASET_PATH, limit=LIMIT):
        bst.addPost(pid, ts, score)
        count += 1
        if count % 5000 == 0: print(f"Inserted {count}...")
            
    total_time = time.perf_counter() - start_time
    avg_insertion = total_time / count if count > 0 else 0
    
    print("\n--- PERFORMANCE METRICS ---")
    print(f"Total Time: {total_time:.4f} sec")
    
    # 2. STRUCTURAL METRICS (FAST VERSION)
    print("Calculating Structural Metrics (O(N))...")
    
    # This single function gets Height and BF in one go
    height, total_bf, node_count = get_structural_metrics(bst.root)
    
    avg_bf = total_bf / node_count if node_count > 0 else 0
    
    print(f"Tree Height: {height}")
    print(f"Avg Balance Factor: {avg_bf:.2f}")

    # 3. VISUALIZATION
    metrics = ['Avg Insert (x1k ms)', 'Height', 'Avg Bal. Factor']
    values = [avg_insertion * 1000, height, avg_bf] 
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(metrics, values, color=['darkred', 'red', 'salmon'])
    plt.title(f"BST Metrics (N={count})")
    
    # Add labels
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, round(yval, 2), ha='center', va='bottom')
        
    plt.savefig('bst_metrics_local.png')
    print(f"Chart saved as 'bst_metrics_local.png'")

if __name__ == "__main__":
    main()
