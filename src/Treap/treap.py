import sys
# Adjust this import based on where you run main.py from.
# If running from root 'Treap vs BST Comparison':
from src.models.models import Node, Post


class Treap:
    def __init__(self):
        self.root = None

        # Metrics for the Report [cite: 50]
        self.rotations_count = 0
        self.comparisons = 0

    # ==========================================
    # 1. CORE ROTATION LOGIC [cite: 53]
    # ==========================================
    def _right_rotate(self, y):
        """
        Rotates node 'y' down to the right.
        Used when Left Child's priority > Parent's priority.
        """
        self.rotations_count += 1
        x = y.left
        T2 = x.right

        # Perform rotation
        x.right = y
        y.left = T2

        # Return new root
        return x

    def _left_rotate(self, x):
        """
        Rotates node 'x' down to the left.
        Used when Right Child's priority > Parent's priority.
        """
        self.rotations_count += 1
        y = x.right
        T2 = y.left

        # Perform rotation
        y.left = x
        x.right = T2

        # Return new root
        return y

    # ==========================================
    # 2. INSERTION
    # ==========================================
    def addPost(self, post_id, timestamp, score):
        new_post = Post(post_id, timestamp, score)
        self.root = self._insert_recursive(self.root, new_post)

    def _insert_recursive(self, node, new_post):
        # 1. Standard BST Insert
        if not node:
            return Node(new_post)

        self.comparisons += 1

        # Insert based on Timestamp (BST Property)
        if new_post.timestamp < node.post.timestamp:
            node.left = self._insert_recursive(node.left, new_post)

            # 2. Fix Heap Property (Max-Heap based on Score)
            if node.left.priority > node.priority:
                node = self._right_rotate(node)

        else:
            node.right = self._insert_recursive(node.right, new_post)

            # 2. Fix Heap Property
            if node.right.priority > node.priority:
                node = self._left_rotate(node)

        return node

    # ==========================================
    # 3. UPDATE (LIKE)
    # ==========================================
    def likePost(self, post_id):
        """
        Increases score and Re-heapifies.
        This function is trickier than BST because changing score
        might force us to rotate the node all the way to the top.
        """
        self.root, found = self._like_recursive(self.root, post_id)
        return found

    def _like_recursive(self, node, post_id):
        if not node:
            return None, False

        # Search Phase (O(N) because ID is not the Key)
        if node.post.post_id == post_id:
            # Update Score
            node.post.score += 1
            node.priority += 1  # Sync priority
            return node, True

        # Search Left
        left_result, found = self._like_recursive(node.left, post_id)
        if found:
            node.left = left_result
            # Check Heap Property on the way back UP (Reheapify)
            if node.left.priority > node.priority:
                node = self._right_rotate(node)
            return node, True

        # Search Right
        right_result, found = self._like_recursive(node.right, post_id)
        if found:
            node.right = right_result
            # Check Heap Property on the way back UP
            if node.right.priority > node.priority:
                node = self._left_rotate(node)
            return node, True

        return node, False

    # ==========================================
    # 4. DELETION
    # ==========================================
    def deletePost(self, post_id):
        self.root = self._delete_recursive(self.root, post_id)

    def _delete_recursive(self, node, post_id):
        if not node: return None

        # 1. Search for the node
        if node.post.post_id == post_id:
            # Found it! Now move it down to a leaf.

            # Case A: It's already a leaf
            if not node.left and not node.right:
                return None

            # Case B: Two children - Rotate with the larger child
            # (We want to preserve the Max-Heap property for the remaining nodes)
            if node.left and node.right:
                if node.left.priority > node.right.priority:
                    node = self._right_rotate(node)
                    node.right = self._delete_recursive(node.right, post_id)
                else:
                    node = self._left_rotate(node)
                    node.left = self._delete_recursive(node.left, post_id)

            # Case C: One child
            elif node.left:
                node = self._right_rotate(node)
                node.right = self._delete_recursive(node.right, post_id)
            else:
                node = self._left_rotate(node)
                node.left = self._delete_recursive(node.left, post_id)

        else:
            # Continue Search (Simulating Search-by-ID overhead)
            # Note: A real production system would use a Hash Map to find the node,
            # then delete it. For this algorithmic study, we traverse.
            if self._contains(node.left, post_id):
                node.left = self._delete_recursive(node.left, post_id)
            else:
                node.right = self._delete_recursive(node.right, post_id)

        return node

    def _contains(self, node, post_id):
        # Helper to guide recursion direction (inefficient but necessary without Hash Map)
        if not node: return False
        if node.post.post_id == post_id: return True
        return self._contains(node.left, post_id) or self._contains(node.right, post_id)

    # ==========================================
    # 5. RETRIEVAL
    # ==========================================
    def getMostPopular(self):
        """
        Return post with highest popularity.
        TREAP ADVANTAGE: This is O(1). The root is always the max priority.

        """
        if self.root:
            return self.root.post
        return None

    def getMostRecent(self, k):
        # Same logic as BST (Reverse In-Order)
        results = []
        self._reverse_inorder(self.root, k, results)
        return results

    def _reverse_inorder(self, node, k, results):
        if not node or len(results) >= k:
            return
        self._reverse_inorder(node.right, k, results)
        if len(results) < k:
            results.append(node.post)
        self._reverse_inorder(node.left, k, results)