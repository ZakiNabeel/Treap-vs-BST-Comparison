import sys
from src.Models.models import Node

class BST:
    def __init__(self):
        self.root = None

        # Performance Counters (for the Report)
        self.comparisons = 0
        self.operations_count = 0

    def addPost(self, post_id, timestamp, score):
        from src.Models.models import Post  # Import here to avoid circular dependencies if any
        new_post = Post(post_id, timestamp, score)

        if not self.root:
            self.root = Node(new_post)
        else:
            self._insert_recursive(self.root, new_post)

    def _insert_recursive(self, node, new_post):
        self.comparisons += 1

        # STANDARD BST LOGIC: Compare only Timestamps
        if new_post.timestamp < node.post.timestamp:
            if node.left is None:
                node.left = Node(new_post)
            else:
                self._insert_recursive(node.left, new_post)
        else:
            # If timestamp is greater OR EQUAL, go right.
            # (Chronological feeds go right, creating the "Stick")
            if node.right is None:
                node.right = Node(new_post)
            else:
                self._insert_recursive(node.right, new_post)

    def likePost(self, post_id):
        """
        Increases score.
        CRITICAL FOR REPORT: In a regular BST, changing the 'score'
        does NOT trigger a rotation or structural change.
        """
        target_node = self._search_by_id(self.root, post_id)
        if target_node:
            target_node.post.score += 1
            # No re-balancing logic here!
            return True
        return False

    def deletePost(self, post_id):
        # This is tricky in a Time-ordered BST because we are deleting by ID.
        # We have to search the whole tree to find the node, then delete it.
        self.root = self._delete_recursive(self.root, post_id)

    def _delete_recursive(self, node, post_id):
        if node is None:
            return None

        # 1. Search Phase (Since we don't know the Timestamp, we must look everywhere)
        # Note: This O(N) traversal is a major weakness you will report.
        if node.post.post_id == post_id:
            # Found it! Perform Standard BST Delete

            # Case 1: No children (Leaf)
            if node.left is None and node.right is None:
                return None

            # Case 2: One child
            if node.left is None: return node.right
            if node.right is None: return node.left

            # Case 3: Two children
            # Find In-order Successor (Smallest in Right Subtree)
            temp = self._get_min_node(node.right)
            # Copy data
            node.post = temp.post
            node.priority = temp.priority  # Keep consistency
            # Delete the successor
            node.right = self._delete_recursive_by_obj(node.right, temp)
            return node

        # If not found yet, keep looking
        # Optimization: If we knew the timestamp, we could search O(log N).
        # But we only have ID. So we must recursively check both sides or use a helper map.
        # For this assignment, we recursively update links.
        node.left = self._delete_recursive(node.left, post_id)
        node.right = self._delete_recursive(node.right, post_id)
        return node

    def _delete_recursive_by_obj(self, node, target_node):
        # Helper to remove the specific successor object
        if node is None: return None
        if node == target_node:
            return node.right
        node.left = self._delete_recursive_by_obj(node.left, target_node)
        return node

    def _get_min_node(self, node):
        current = node
        while current.left is not None:
            current = current.left
        return current

    def _search_by_id(self, node, post_id):
        # Helper: O(N) search because BST is not ordered by ID
        if not node: return None
        if node.post.post_id == post_id: return node

        res1 = self._search_by_id(node.left, post_id)
        if res1: return res1
        return self._search_by_id(node.right, post_id)

    def getMostPopular(self):
        """
        Finds the post with highest score.
        BST WEAKNESS: Must scan O(N) nodes because max score
        could be anywhere.
        """
        if not self.root: return None

        self.max_post = self.root.post
        self._find_max_score_recursive(self.root)
        return self.max_post

    def _find_max_score_recursive(self, node):
        if not node: return
        if node.post.score > self.max_post.score:
            self.max_post = node.post

        self._find_max_score_recursive(node.left)
        self._find_max_score_recursive(node.right)

    def getMostRecent(self, k):
        """
        Returns k-most recent posts.
        Efficient! This is Reverse In-Order Traversal (Right -> Root -> Left).
        """
        results = []
        self._reverse_inorder(self.root, k, results)
        return results

    def _reverse_inorder(self, node, k, results):
        if not node or len(results) >= k:
            return

        # 1. Go Right (Newest)
        self._reverse_inorder(node.right, k, results)

        # 2. Visit Node
        if len(results) < k:
            results.append(node.post)

        # 3. Go Left (Older)
        self._reverse_inorder(node.left, k, results)