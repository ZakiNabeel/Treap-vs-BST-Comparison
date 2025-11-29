import json
import os
import io
import zstandard as zstd
import matplotlib.pyplot as plt


# --- DATA STREAMING ---
def stream_reddit_dataset(filepath, limit=None):
    if not os.path.exists(filepath):
        print(f"Error: File not found at {filepath}")
        return

    with open(filepath, 'rb') as fh:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            count = 0
            for line in text_stream:
                if limit and count >= limit: break
                try:
                    data = json.loads(line)
                    yield (data['id'], int(data['created_utc']), int(data['score']))
                    count += 1
                except:
                    continue


# --- NEW: METRIC CALCULATIONS ---
def get_tree_height(node):
    if not node: return 0
    return 1 + max(get_tree_height(node.left), get_tree_height(node.right))


def get_total_balance_factor(node):
    """
    Recursively sums the ABSOLUTE balance factor of every node.
    BF = abs(Height(Left) - Height(Right))
    """
    if not node: return 0, 0  # (Sum of BF, Count of Nodes)

    h_left = get_tree_height(node.left)
    h_right = get_tree_height(node.right)

    current_bf = abs(h_left - h_right)

    left_sum, left_count = get_total_balance_factor(node.left)
    right_sum, right_count = get_total_balance_factor(node.right)

    return (current_bf + left_sum + right_sum), (1 + left_count + right_count)


def print_ascii_tree(node, prefix="", is_left=True):
    if node is not None:
        print(prefix + ("|-- " if is_left else "`-- ") + f"[{node.post.post_id}]")
        print_ascii_tree(node.left, prefix + ("|   " if is_left else "    "), True)
        print_ascii_tree(node.right, prefix + ("|   " if is_left else "    "), False)