from src.Models.models import Node, Post


class Treap:
    """
    A Randomized Binary Search Tree (Treap) implementation.

    Structure Properties:
    1. BST Property: Ordered by 'timestamp' (In-order traversal gives chronological order).
    2. Heap Property: Ordered by 'score'/priority (Max-Heap: Root has highest score).
    """

    def __init__(self):
        self.root = None
        # Performance Metrics
        self.rotations_count = 0  # To measure rebalancing activity
        self.comparisons = 0  # To measure search effort

    # ==========================================
    # 1. CORE ROTATION LOGIC
    # ==========================================
    def _right_rotate(self, y):
        """
        Performs a Right Rotation.
        Triggered when a Left Child's priority is higher than the Parent.
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
        Performs a Left Rotation.
        Triggered when a Right Child's priority is higher than the Parent.
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
        """
        Inserts a new post into the Treap.
        1. Insert as normal BST leaf based on Timestamp.
        2. Rotate up if Score violates Heap property.
        """
        new_post = Post(post_id, timestamp, score)
        self.root = self._insert_recursive(self.root, new_post)

    def _insert_recursive(self, node, new_post):
        # Base case: Found insertion spot
        if not node:
            return Node(new_post)

        self.comparisons += 1

        # 1. BST Insertion (Sorting by Timestamp)
        if new_post.timestamp < node.post.timestamp:
            node.left = self._insert_recursive(node.left, new_post)

            # 2. Fix Heap Property (Max-Heap)
            # If child has higher score than parent, rotate right
            if node.left.priority > node.priority:
                node = self._right_rotate(node)
        else:
            node.right = self._insert_recursive(node.right, new_post)

            # 2. Fix Heap Property
            # If child has higher score than parent, rotate left
            if node.right.priority > node.priority:
                node = self._left_rotate(node)

        return node

    # ==========================================
    # 3. UPDATE (LIKE)
    # ==========================================
    def likePost(self, post_id):
        """
        Increments the score of a post and re-balances the tree.
        Unlike a BST, changing the score in a Treap requires structural changes.
        """
        self.root, found = self._like_recursive(self.root, post_id)
        return found

    def _like_recursive(self, node, post_id):
        if not node: return None, False

        # Target Found
        if node.post.post_id == post_id:
            node.post.score += 1
            node.priority += 1  # Priority matches score
            return node, True

        # Recursive Search (Note: O(N) because searching by ID, not Timestamp)
        # Search Left
        left_res, found = self._like_recursive(node.left, post_id)
        if found:
            node.left = left_res
            # Reheapify: If left child is now larger, rotate right
            if node.left.priority > node.priority:
                node = self._right_rotate(node)
            return node, True

        # Search Right
        right_res, found = self._like_recursive(node.right, post_id)
        if found:
            node.right = right_res
            # Reheapify: If right child is now larger, rotate left
            if node.right.priority > node.priority:
                node = self._left_rotate(node)
            return node, True

        return node, False

    # ==========================================
    # 4. DELETION
    # ==========================================
    def deletePost(self, post_id):
        """
        Removes a post.
        Strategy: Locate node -> Rotate it down to leaf -> Remove.
        """
        self.root = self._delete_recursive(self.root, post_id)

    def _delete_recursive(self, node, post_id):
        if not node: return None

        # 1. Node Found
        if node.post.post_id == post_id:
            # Case A: Leaf Node (Just delete)
            if not node.left and not node.right:
                return None

            # Case B: Two Children or One Child
            # Rotate with the HIGHER priority child to preserve Max-Heap
            if node.left and node.right:
                if node.left.priority > node.right.priority:
                    node = self._right_rotate(node)
                    # Node is now in right subtree, continue deleting
                    node.right = self._delete_recursive(node.right, post_id)
                else:
                    node = self._left_rotate(node)
                    # Node is now in left subtree, continue deleting
                    node.left = self._delete_recursive(node.left, post_id)

            # Case C: Only Left Child
            elif node.left:
                node = self._right_rotate(node)
                node.right = self._delete_recursive(node.right, post_id)

            # Case D: Only Right Child
            else:
                node = self._left_rotate(node)
                node.left = self._delete_recursive(node.left, post_id)

        else:
            # 2. Search Phase
            # (We check children recursively to simulate finding by ID)
            # Optimization: In a real system, we'd use a Hash Map to find the node location immediately.
            if self._contains(node.left, post_id):
                node.left = self._delete_recursive(node.left, post_id)
            else:
                node.right = self._delete_recursive(node.right, post_id)
        return node

    def _contains(self, node, post_id):
        # Helper to guide recursion direction (inefficient O(N) but necessary without Hash Map)
        if not node: return False
        if node.post.post_id == post_id: return True
        return self._contains(node.left, post_id) or self._contains(node.right, post_id)

    # ==========================================
    # 5. RETRIEVAL
    # ==========================================
    def getMostPopular(self):
        """
        O(1) Retrieval. 
        In a Treap, the root is always the item with the highest priority (Score).
        """
        return self.root.post if self.root else None

    # ==========================================
    # 6. BONUS: UNION & SPLIT (Merge Logic)
    # ==========================================
    def split(self, root, key):
        """
        Splits the tree into two trees based on Timestamp Key:
        1. Left Tree: All Timestamps <= key
        2. Right Tree: All Timestamps > key
        """
        if not root:
            return None, None

        if root.post.timestamp <= key:
            # Root and its left subtree belong to the Left Split.
            # We recurse to the right to find the cutoff.
            right_split_left, right_split_right = self.split(root.right, key)
            root.right = right_split_left
            return root, right_split_right
        else:
            # Root and its right subtree belong to the Right Split.
            # We recurse to the left.
            left_split_left, left_split_right = self.split(root.left, key)
            root.left = left_split_right
            return left_split_left, root

    def union(self, other_treap):
        """
        Merges 'other_treap' (Treap object) into THIS treap.
        Used for the n-Treaps Partitioning strategy.
        """
        if not other_treap or not other_treap.root:
            return

        self.root = self._union_recursive(self.root, other_treap.root)
        # Sum metrics for reporting
        self.rotations_count += other_treap.rotations_count

    def _union_recursive(self, t1, t2):
        """
        Recursive helper to merge two Treap Nodes (t1 and t2).
        Algorithm:
        1. Pick the node with higher priority as the new Root.
        2. Split the other tree based on that Root's timestamp.
        3. Recursively merge children.
        """
        if not t1: return t2
        if not t2: return t1

        # Ensure t1 is the root with higher priority (Heap Property)
        if t1.priority < t2.priority:
            t1, t2 = t2, t1  # Swap

        # Now Split t2 based on t1's key (Timestamp)
        left_t2, right_t2 = self.split(t2, t1.