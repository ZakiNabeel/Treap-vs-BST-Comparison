import json
import os
import io
import zstandard as zstd
import sys

# Increase recursion depth just in case, though the optimized metric is iterative/safe
sys.setrecursionlimit(200000)

# --- 1. DATA STREAMING ---
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
                except: continue

# --- 2. OPTIMIZED METRIC CALCULATION (O(N)) ---
# This is the function that was missing!
def get_structural_metrics(node):
    """
    Calculates Height and Balance Factor in a SINGLE pass (Bottom-Up).
    Returns: (Height, Total_Abs_Balance_Factor, Node_Count)
    """
    if not node:
        return 0, 0, 0 # Height, Total_BF, Count

    # Recursive Step
    l_h, l_bf_sum, l_count = get_structural_metrics(node.left)
    r_h, r_bf_sum, r_count = get_structural_metrics(node.right)

    # Calculate Current Node Stats
    current_height = 1 + max(l_h, r_h)
    current_bf = abs(l_h - r_h) # Balance Factor = |LeftH - RightH|
    
    # Aggregate Stats
    total_bf = current_bf + l_bf_sum + r_bf_sum
    total_count = 1 + l_count + r_count

    return current_height, total_bf, total_count
