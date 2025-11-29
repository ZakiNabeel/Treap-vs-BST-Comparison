import time
import matplotlib.pyplot as plt
import os

# Clean Import
from src.Treap.treap import Treap
from src.Utility.utils import stream_reddit_dataset, get_tree_height, get_total_balance_factor

# --- CONFIGURATION ---
LOCAL_MODE = False  # Set to False when on Kaggle

if LOCAL_MODE:
    DATASET_PATH = "dummy_data.json"
    LIMIT = 1000
else:
    DATASET_PATH = "/kaggle/input/the-pushshift-reddit-dataset-submissions/RC_2019-04.zst"
    LIMIT = 5000


def main():
    print(f"=== TREAP IMPLEMENTATION (Processing {LIMIT} nodes) ===")
    treap = Treap()

    # 1. INSERTION PHASE
    print(f"Streaming data from: {DATASET_PATH}...")
    start_time = time.perf_counter()
    count = 0

    if not os.path.exists(DATASET_PATH):
        print(f"Error: Dataset not found at {DATASET_PATH}")
        return

    for pid, ts, score in stream_reddit_dataset(DATASET_PATH, limit=LIMIT):
        treap.addPost(pid, ts, score)
        count += 1
        if count % 1000 == 0: print(f"Inserted {count}...")

    total_time = time.perf_counter() - start_time
    avg_insertion = total_time / count if count > 0 else 0

    print("\n--- PERFORMANCE METRICS ---")
    print(f"Total Time: {total_time:.4f} sec")
    print(f"Avg Insertion Time: {avg_insertion:.8f} sec")
    print(f"Total Rotations: {treap.rotations_count}")

    # [cite_start]2. STRUCTURAL METRICS [cite: 50]
    print("Calculating Height...")
    height = get_tree_height(treap.root)
    print(f"Tree Height: {height}")

    print("Calculating Balance Factor...")
    total_bf, node_count = get_total_balance_factor(treap.root)
    avg_bf = total_bf / node_count if node_count > 0 else 0
    print(f"Avg Balance Factor: {avg_bf:.2f}")

    # [cite_start]3. VISUALIZATION [cite: 53]
    metrics = ['Avg Insert (x1k ms)', 'Height', 'Avg Bal. Factor']
    values = [avg_insertion * 1000, height, avg_bf]

    plt.figure(figsize=(10, 6))
    bars = plt.bar(metrics, values, color=['darkblue', 'blue', 'skyblue'])
    plt.title(f"Treap Metrics (N={count})")
    plt.ylabel('Value')

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')

    output_file = 'treap_metrics_local.png'
    plt.savefig(output_file)
    print(f"Chart saved as '{output_file}'")


if __name__ == "__main__":
    main()
