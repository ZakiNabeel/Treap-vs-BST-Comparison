import json
import time
import os
import zstandard as zstd
import io
import matplotlib.pyplot as plt


def stream_reddit_dataset(filepath, limit=None):
    """
    Generator that yields posts one by one from the compressed .zst file.
    Implements the 'Stream Processing' strategy.
    """
    if not os.path.exists(filepath):
        print(f"Error: File not found at {filepath}")
        return

    with open(filepath, 'rb') as fh:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')

            count = 0
            for line in text_stream:
                if limit and count >= limit:
                    break
                try:
                    data = json.loads(line)
                    # Yield tuple: (id, timestamp, score)
                    yield (data['id'], int(data['created_utc']), int(data['score']))
                    count += 1
                except (json.JSONDecodeError, KeyError):
                    continue


def print_tree_structure(node, level=0, prefix="Root: "):
    """
    Recursive function to print the tree visually.
    Helps verify if BST is a 'Stick' and Treap is balanced.
    """
    if node is not None:
        print(" " * (level * 4) + prefix + str(node.post))
        if node.left or node.right:
            if node.left:
                print_tree_structure(node.left, level + 1, "L--- ")
            else:
                print(" " * ((level + 1) * 4) + "L--- None")

            if node.right:
                print_tree_structure(node.right, level + 1, "R--- ")
            else:
                print(" " * ((level + 1) * 4) + "R--- None")


def run_mandated_test_case(tree, tree_name):
    """
    Executes the EXACT test case from the Project PDF (Page 5-6).
    """
    print(f"\n=== Running Mandated Test Case on {tree_name} ===")

    # 1. Add Posts [cite: 55-64]
    print("Step 1: Adding Posts...")
    posts = [
        ("ejualnb", 1554076800, 55),
        ("ejualnc", 1554076800, 12),
        ("ejualnd", 1554076800, 27),
        ("ejualne", 1554076800, 14),
        ("ejualnl", 1554076809, 13)
    ]

    for pid, ts, score in posts:
        tree.addPost(pid, ts, score)
        print(f"  Inserted {pid} (Score: {score})")

    print(f"\nSnapshot of {tree_name} after insertion:")
    print_tree_structure(tree.root)

    # 2. Like Post 'ejualnl' 2 times [cite: 68-70]
    print("\nStep 2: Liking 'ejualnl' 2 times...")
    tree.likePost("ejualnl")
    tree.likePost("ejualnl")
    print("  'ejualnl' liked twice. (Expected Score: 15)")

    # Check structure update
    if tree_name == "Treap":
        print("  (Treap should have rotated 'ejualnl' up if priority increased)")
    print_tree_structure(tree.root)

    # 3. Delete Post 'ejualnc' [cite: 78]
    print("\nStep 3: Deleting 'ejualnc'...")
    tree.deletePost("ejualnc")
    print_tree_structure(tree.root)

    # 4. Get Most Popular [cite: 83]
    print("\nStep 4: Get Most Popular...")
    popular = tree.getMostPopular()
    print(f"  Result: {popular}")
    print(f"  Expected: ('ejualnb', 1554076800, 55) (or higher if updates occurred)")


def plot_performance_graphs(bst_metrics, treap_metrics):
    """
    Generates the comparison graphs required for the report.
    metrics = list of execution times or counts.
    """
    # Example: Plotting Insertion Time
    plt.figure(figsize=(10, 5))
    plt.plot(bst_metrics['insert_times'], label='BST Insert Time', color='red')
    plt.plot(treap_metrics['insert_times'], label='Treap Insert Time', color='blue')
    plt.xlabel('Number of Posts')
    plt.ylabel('Time (seconds)')
    plt.title('Insertion Performance: BST vs Treap')
    plt.legend()
    plt.grid(True)
    plt.savefig('insertion_comparison.png')
    print("Graph saved: insertion_comparison.png")