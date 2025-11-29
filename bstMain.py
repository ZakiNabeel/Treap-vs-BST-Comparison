import time
import matplotlib.pyplot as plt
import os

# Clean Import (Assumes you renamed 'Binary Search Tree' -> 'BinarySearchTree')
from src.BinarySearchTree.bst import BST
from src.Utility.utils import stream_reddit_dataset, get_tree_height, get_total_balance_factor

# --- CONFIGURATION ---
LOCAL_MODE = True  # Set to False when on Kaggle

if LOCAL_MODE:
    # Ensure you have a 'dummy_data.json' or a local copy of the ZST
    DATASET_PATH = "dummy_data.json"
    LIMIT = 1000  # Smaller limit for local testing
else:
    DATASET_PATH = "/kaggle/input/the-pushshift-reddit-dataset-submissions/RC_2019-04.zst"
    LIMIT = 5000


def main():
    print(f"=== BST IMPLEMENTATION (Processing {LIMIT} nodes) ===")
    bst = BST()

    # [cite_start]1. INSERTION PHASE [cite: 50]
    print(f"Streaming data from: {DATASET_PATH}...")
    start_time = time.perf_counter()
    count = 0

    # Handle case where file might not exist locally yet
    if not os.path.exists(DATASET_PATH):
        print(f"Error: Dataset not found at {DATASET_PATH}")
        print("Tip: Create a 'dummy_data.json' file for local testing.")
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

    # [cite_start]2. STRUCTURAL METRICS [cite: 47]
    print("Calculating Height (This may be slow for skewed BST)...")
    # Increase recursion limit for BSTs which can get very deep (the 'Stick')
    import sys
    sys.setrecursionlimit(max(2000, LIMIT + 500))

    height = get_tree_height(bst.root)
    print(f"Tree Height: {height}")

    print("Calculating Balance Factor...")
    total_bf, node_count = get_total_balance_factor(bst.root)
    avg_bf = total_bf / node_count if node_count > 0 else 0
    print(f"Avg Balance Factor: {avg_bf:.2f}")

    # [cite_start]3. VISUALIZATION [cite: 48]
    metrics = ['Avg Insert (x1k ms)', 'Height', 'Avg Bal. Factor']
    # Scale time (x1000) so it's visible next to Height
    values = [avg_insertion * 1000, height, avg_bf]

    plt.figure(figsize=(10, 6))
    bars = plt.bar(metrics, values, color=['darkred', 'red', 'salmon'])
    plt.title(f"BST Metrics (N={count})")
    plt.ylabel('Value')

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')

    output_file = 'bst_metrics_local.png'
    plt.savefig(output_file)
    print(f"Chart saved as '{output_file}'")


if __name__ == "__main__":
    main()