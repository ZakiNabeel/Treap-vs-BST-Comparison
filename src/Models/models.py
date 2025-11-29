class Post:
    """
    Represents a single social media post.
    This object is the 'Payload' stored inside our Tree Nodes.
    """

    def __init__(self, post_id, timestamp, score):
        self.post_id = str(post_id)  # Unique ID (e.g., "ejualnb")
        self.timestamp = int(timestamp)  # The Key for BST ordering (Time)
        self.score = int(score)  # The Priority for Treap balancing (Popularity)

    def __repr__(self):
        # formatted for easy reading: (ID, Time, Score)
        return f"('{self.post_id}', {self.timestamp}, {self.score})"


class Node:
    """
    A generic Tree Node that works for both BST and Treap.
    """

    def __init__(self, post):
        self.post = post  # The data payload
        self.left = None  # Left Child
        self.right = None  # Right Child

        # TREAP SPECIFIC:
        self.priority = post.score

        # METRICS & UTILS:
        self.height = 1

    def __repr__(self):
        return f"Node({self.post})"