import time
import matplotlib.pyplot as plt
import os
import sys
import importlib.util

# ==========================================
# FIX FOR FOLDER WITH SPACES ("Binary Search Tree")
# ==========================================
# We cannot use 'from src.Binary Search Tree import BST'
# So we load the file path manually.
try:
    # Build the path: src -> Binary Search Tree -> bst.py
    bst_path = os.path.join("src", "Binary Search Tree", "bst.py")
    
    # Load the module dynamically
    spec = importlib.util.spec_from_file_location("bst_module", bst_path)
    bst_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bst_module)
    BST = bst_module.BST
except FileNotFoundError:
    print("CRITICAL ERROR: Could not find 'src/Binary Search Tree/bst.py'")
    print("Please check if the folder name matches exactly.")
    sys.exit(1)

# Import Utils (This one usually doesn't have spaces)
from src.Utility.utils import stream_reddit_dataset, get_tree_height, get_total_balance_factor

# --- CONFIGURATION ---
# We force Kaggle mode since you are running this command on Kaggle
DATASET_PATH = "/kaggle/input/the-pushshift-reddit-dataset-submissions/RC_2019-04.zst"
LIMIT = 50000 # Processing a large chunk as requested

def main():
    print(f"=== BST IMPLEMENTATION (Processing {LIMIT} nodes) ===")
    bst = BST()
    
    # 1. INSERTION PHASE
    print(f"Streaming data from: {DATASET_PATH}...")
    start_time = time.perf_counter()
    count = 0
    
    if not os.path.exists(DATASET_PATH):
        print(f"Error: Dataset not found at {DATASET_PATH}")
        return

    for pid, ts, score in stream_reddit_dataset(DATASET_PATH, limit=LIMIT):
        bst.addPost(pid, ts, score)
        count += 1
        if count % 1000 == 0: print(f"Inserted {count}...")
            
    total_time = time.perf_counter() - start_time
    avg_insertion = total_time / count if count > 0 else 0
    
    print("\n--- PERFORMANCE METRICS ---")
    print(f"Total Time: {total_time:.4f} sec")
    print(f"Avg Insertion Time: {avg_insertion:.8f} sec")
    
    # 2. STRUCTURAL METRICS
    print("Calculating Height (This may be slow for skewed BST)...")
    # Increase recursion limit because the BST will be a giant stick
    sys.setrecursionlimit(max(2000, LIMIT + 500))
    
    height = get_tree_height(bst.root)
    print(f"Tree Height: {height}")
    
    print("Calculating Balance Factor...")
    total_bf, node_count = get_total_balance_factor(bst.root)
    avg_bf = total_bf / node_count if node_count > 0 else 0
    print(f"Avg Balance Factor: {avg_bf:.2f}")

    # 3. VISUALIZATION
    metrics = ['Avg Insert (x1k ms)', 'Height', 'Avg Bal. Factor']
    values = [avg_insertion * 1000, height, avg_bf] 
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(metrics, values, color=['darkred', 'red', 'salmon'])
    plt.title(f"BST Metrics (N={count})")
    plt.ylabel('Value')
    
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, round(yval, 2), ha='center', va='bottom')
        
    output_file = 'bst_metrics_local.png'
    plt.savefig(output_file)
    print(f"Chart saved as '{output_file}'")

if __name__ == "__main__":
    main()
