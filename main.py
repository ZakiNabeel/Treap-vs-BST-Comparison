import time
import random
import matplotlib.pyplot as plt

# Imports from your src structure
from src.Treap.treap import Treap
from src.BinarySearchTree.bst import BST
from src.Utility.utils import stream_reddit_dataset, run_mandated_test_case, plot_performance_graphs

# CONFIGURATION
DATASET_PATH = "/kaggle/input/the-pushshift-reddit-dataset-submissions/RC_2019-04.zst"


# ^ Change this path if running locally on your laptop (e.g., "RC_2019-04.zst")

def run_simulation(limit=10000):
    """
    Runs the large-scale ablation study[cite: 46].
    Compares BST vs Treap on Insert, Search, and Update operations.
    """
    print(f"\n=== STARTING SIMULATION (Limit: {limit} posts) ===")

    # 1. Initialize Structures
    my_treap = Treap()
    my_bst = BST()

    # Metrics Storage
    metrics = {
        'x_axis': [],
        'bst_insert_time': [],
        'treap_insert_time': [],
        'bst_height_est': [],  # Estimated by depth of recent inserts
        'treap_rotations': []
    }

    # Track IDs to simulate "Liking" existing posts later
    active_post_ids = []

    print("Streaming data...")
    count = 0

    # 2. Process the Stream
    start_total = time.time()

    for pid, ts, score in stream_reddit_dataset(DATASET_PATH, limit=limit):
        count += 1
        active_post_ids.append(pid)

        # --- MEASURE BST INSERT ---
        t0 = time.perf_counter()
        my_bst.addPost(pid, ts, score)
        t1 = time.perf_counter()
        bst_time = t1 - t0

        # --- MEASURE TREAP INSERT ---
        t0 = time.perf_counter()
        my_treap.addPost(pid, ts, score)
        t1 = time.perf_counter()
        treap_time = t1 - t0

        # --- SIMULATE REAL-TIME UPDATES (LIKES) [cite: 13] ---
        # Every 100 insertions, simulate 10 likes on random existing posts
        if count % 100 == 0:
            for _ in range(10):
                if active_post_ids:
                    random_id = random.choice(active_post_ids)
                    my_bst.likePost(random_id)
                    my_treap.likePost(random_id)

        # --- RECORD METRICS (Every 1000 steps to save memory) ---
        if count % 1000 == 0:
            print(f"Processed {count} posts...")
            metrics['x_axis'].append(count)
            metrics['bst_insert_time'].append(bst_time)
            metrics['treap_insert_time'].append(treap_time)
            metrics['treap_rotations'].append(my_treap.rotations_count)

    total_time = time.time() - start_total
    print(f"\nSimulation Complete in {total_time:.2f} seconds.")

    # 3. Final Comparative Analysis [cite: 40]
    print("\n=== FINAL RESULTS ===")

    # Compare "Get Most Popular" (The Critical Test)
    t0 = time.perf_counter()
    pop_bst = my_bst.getMostPopular()
    bst_pop_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    pop_treap = my_treap.getMostPopular()
    treap_pop_time = time.perf_counter() - t0

    print(f"{'Metric':<25} | {'BST':<15} | {'Treap':<15}")
    print("-" * 60)
    print(f"{'Total Comparisons':<25} | {my_bst.comparisons:<15} | {my_treap.comparisons:<15}")
    print(f"{'Total Rotations':<25} | {0:<15} | {my_treap.rotations_count:<15}")
    print(f"{'GetMostPopular Time':<25} | {bst_pop_time:.6f}s       | {treap_pop_time:.6f}s")
    print("-" * 60)

    if pop_treap:
        print(f"Most Popular Post: {pop_treap.post_id} (Score: {pop_treap.score})")

    # 4. Generate Graphs [cite: 48]
    # (Simple plot example)
    plt.figure(figsize=(12, 6))
    plt.plot(metrics['x_axis'], metrics['bst_insert_time'], label='BST Insert Time', color='red', alpha=0.6)
    plt.plot(metrics['x_axis'], metrics['treap_insert_time'], label='Treap Insert Time', color='blue', alpha=0.6)
    plt.title('Performance Comparison: BST (Stick) vs Treap (Balanced)')
    plt.xlabel('Number of Posts Inserted')
    plt.ylabel('Time per Operation (seconds)')
    plt.legend()
    plt.savefig('comparison_result.png')
    print("Graph saved as 'comparison_result.png'")


def main():
    print("==================================================")
    print("Social Media Feed Algorithm Case Study")
    print("1. Run Mandated Test Case (Small Data check) [cite: 54]")
    print("2. Run Full Simulation (Large Data Stream) [cite: 5]")
    print("==================================================")

    choice = input("Enter choice (1 or 2): ")

    if choice == '1':
        # Create fresh instances for the test case
        t = Treap()
        b = BST()

        # Run Treap Test
        run_mandated_test_case(t, "Treap")

        # Run BST Test (to show the difference in structure)
        print("\n" + "=" * 30 + "\n")
        run_mandated_test_case(b, "BST")

    elif choice == '2':
        limit = input("Enter number of posts to process (default 5000): ")
        limit = int(limit) if limit.isdigit() else 5000
        run_simulation(limit)
    else:
        print("Invalid choice.")


if __name__ == "__main__":
    main()