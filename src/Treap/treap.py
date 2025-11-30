%%writefile src/Treap/treap.py
import sys
# Standard imports with fallback for different folder structures
try:
    from src.Models.models import Node, Post
except ImportError:
    from src.models.models import Node, Post

class Treap:
    """
    A Randomized Binary Search Tree (Treap) Data Structure.
    
    Theoretical Foundation:
    - Acts as a BST with respect to the 'Timestamp' (Key).
    - Acts as a Max-Heap with respect to the 'Score' (Priority).
    - Probabilistic Balancing: Random/Arbitrary scores ensure O(log N) height 
      with high probability, preventing degeneration into a linked list.
    """
    def __init__(self):
        self.root = None
        # Performance Counters for Ablation Study
        self.rotations_count = 0  # Measures structural volatility
        self.comparisons = 0      # Measures search cost

    # ==========================================
    # 1. CORE ROTATION LOGIC (O(1))
    # ==========================================
    def _right_rotate(self, y):
        """
        Performs a Right Rotation to fix Heap Property violation.
        Used when a Left Child's priority > Parent's priority.
        
        y (Parent) becomes right child of x (Left Child).
        """
        self.rotations_count += 1
        x = y.left
        T2 = x.right
        
        # Rotation steps
        x.right = y
        y.left = T2
        
        return x # New root of this subtree

    def _left_rotate(self, x):
        """
        Performs a Left Rotation to fix Heap Property violation.
        Used when a Right Child's priority > Parent's priority.
        
        x (Parent) becomes left child of y (Right Child).
        """
        self.rotations_count += 1
        y = x.right
        T2 = y.left
        
        # Rotation steps
        y.left = x
        x.right = T2
        
        return y # New root of this subtree

    # ==========================================
    # 2. INSERTION (Expected O(log N))
    # ==========================================
    def addPost(self, post_id, timestamp, score):
        """
        Inserts a new post.
        1. Standard BST insert based on Timestamp.
        2. 'Bubble Up' (Reheapify) using rotations if Score violates Max-Heap.
        """
        new_post = Post(post_id, timestamp, score)
        self.root = self._insert_recursive(self.root, new_post)

    def _insert_recursive(self, node, new_post):
        # 1. Base Case: Found empty spot, insert leaf
        if not node: 
            return Node(new_post)
        
        self.comparisons += 1
        
        # 2. BST Logic: Navigate by Timestamp
        if new_post.timestamp < node.post.timestamp:
            node.left = self._insert_recursive(node.left, new_post)
            
            # 3. Heap Logic: Check Priority & Rotate
            if node.left.priority > node.priority:
                node = self._right_rotate(node)
        else:
            node.right = self._insert_recursive(node.right, new_post)
            
            # 3. Heap Logic: Check Priority & Rotate
            if node.right.priority > node.priority:
                node = self._left_rotate(node)
                
        return node

    # ==========================================
    # 3. UPDATE (LIKE) (Expected O(log N))
    # ==========================================
    def likePost(self, post_id):
        """
        Updates a post's score.
        Crucial distinction from BST: Changing score changes structure.
        We must recursively check and fix the Heap Property.
        """
        self.root, found = self._like_recursive(self.root, post_id)
        return found

    def _like_recursive(self, node, post_id):
        if not node: return None, False
        
        # Case 1: Node Found
        if node.post.post_id == post_id:
            node.post.score += 1
            node.priority += 1 # Priority matches score
            # Rotation check happens in the caller (unwinding recursion)
            return node, True
        
        # Case 2: Search Left
        # Note: We check both sides because we search by ID, not Timestamp
        left_res, found = self._like_recursive(node.left, post_id)
        if found:
            node.left = left_res
            # Reheapify on the way up
            if node.left.priority > node.priority: 
                node = self._right_rotate(node)
            return node, True
            
        # Case 3: Search Right
        right_res, found = self._like_recursive(node.right, post_id)
        if found:
            node.right = right_res
            # Reheapify on the way up
            if node.right.priority > node.priority: 
                node = self._left_rotate(node)
            return node, True
            
        return node, False

    # ==========================================
    # 4. DELETION (Expected O(log N))
    # ==========================================
    def deletePost(self, post_id):
        """
        Deletes a node.
        Strategy: Rotate the node DOWN until it becomes a leaf, then snip it.
        """
        self.root = self._delete_recursive(self.root, post_id)

    def _delete_recursive(self, node, post_id):
        if not node: return None

        if node.post.post_id == post_id:
            # Case A: Leaf Node - Just remove
            if not node.left and not node.right: return None
            
            # Case B: Two Children - Rotate with the larger child (preserve Heap)
            if node.left and node.right:
                if node.left.priority > node.right.priority:
                    node = self._right_rotate(node)
                    node.right = self._delete_recursive(node.right, post_id)
                else:
                    node = self._left_rotate(node)
                    node.left = self._delete_recursive(node.left, post_id)
            
            # Case C: Single Child - Rotate down
            elif node.left:
                node = self._right_rotate(node)
                node.right = self._delete_recursive(node.right, post_id)
            else:
                node = self._left_rotate(node)
                node.left = self._delete_recursive(node.left, post_id)
        else:
            # Search Phase (Recursive Traversal)
            if self._contains(node.left, post_id): 
                node.left = self._delete_recursive(node.left, post_id)
            else: 
                node.right = self._delete_recursive(node.right, post_id)
        return node

    def _contains(self, node, post_id):
        # Helper for search guidance (O(N) in worst case for ID search)
        if not node: return False
        if node.post.post_id == post_id: return True
        return self._contains(node.left, post_id) or self._contains(node.right, post_id)

    # ==========================================
    # 5. RETRIEVAL (O(1))
    # ==========================================
    def getMostPopular(self):
        """
        Returns the post with the highest score.
        Treap Advantage: This is always the Root. O(1) time.
        """
        return self.root.post if self.root else None

    # ==========================================
    # 6. BONUS: UNION & SPLIT (Merge Logic)
    # ==========================================
    def split(self, root, key):
        """
        Splits a Treap into two Treaps based on Key (Timestamp):
        - Left Tree: All nodes with timestamp <= key
        - Right Tree: All nodes with timestamp > key
        Used during Union operations.
        """
        if not root: return None, None
        
        if root.post.timestamp <= key:
            # Root belongs to Left. Recurse right.
            right_split_left, right_split_right = self.split(root.right, key)
            root.right = right_split_left
            return root, right_split_right
        else:
            # Root belongs to Right. Recurse left.
            left_split_left, left_split_right = self.split(root.left, key)
            root.left = left_split_right
            return left_split_left, root

    def union(self, other_treap):
        """
        Merges an external Treap into the current one.
        Algorithm:
        1. Compare roots. Higher priority becomes the new root.
        2. Split the lower-priority tree based on the new root's key.
        3. Recursively merge children.
        Complexity: O(M log(N/M))
        """
        if not other_treap or not other_treap.root: return
        self.root = self._union_recursive(self.root, other_treap.root)
        self.rotations_count += other_treap.rotations_count

    def _union_recursive(self, t1, t2):
        if not t1: return t2
        if not t2: return t1
        
        # Ensure t1 is the root with higher priority
        if t1.priority < t2.priority:
            t1, t2 = t2, t1 
            
        # Split t2 to fit around t1
        left_t2, right_t2 = self.split(t2, t1.post.timestamp)
        
        # Merge subtrees
        t1.left = self._union_recursive(t1.left, left_t2)
        t1.right = self._union_recursive(t1.right, right_t2)
        return t1
