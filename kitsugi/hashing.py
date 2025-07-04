import json
import hashlib

# This constant is used to avoid hashing the hash key itself when processing files
# that have already been modified.
HASH_KEY = "_sha256_hash"

def sha256_hash(text: str) -> str:
    """A simple helper to calculate the SHA-256 hash of a string."""
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

# ==============================================================================
#  The Visitor Pattern
# ==============================================================================
# Defines different "contexts" for the hashing function. This separates the
# core logic of hashing from what is done with the results.

class BaseVisitor:
    """A base class for visitors that act on data during hash traversal."""
    def visit(self, hash_val, node, location_str, is_primitive, parent_hash=None, child_key=None):
        """This method is called for every node after its hash is calculated."""
        pass

class AnalysisVisitor(BaseVisitor):
    """A visitor that simply collects all unique hashes into a set."""
    def __init__(self):
        self.hash_set = set()
    
    def visit(self, hash_val, **kwargs):
        self.hash_set.add(hash_val)

class WriteContextVisitor(BaseVisitor):
    """A visitor that collects all data needed for a database write operation."""
    def __init__(self):
        self.index_data = []
        self.graph_data = []
        self.primitive_data = []

    def visit(self, hash_val, node, location_str, is_primitive, parent_hash=None, child_key=None):
        self.index_data.append((hash_val, location_str))
        
        # If a parent hash and child key are provided, it means we can form a graph link.
        if parent_hash and child_key is not None:
            self.graph_data.append((parent_hash, child_key, hash_val))

        # If the node is a primitive, store its raw data for the hash_to_data table.
        if is_primitive:
             self.primitive_data.append((hash_val, json.dumps(node, ensure_ascii=False)))

# ==============================================================================
#  The Universal Hashing Functor
# ==============================================================================

def calculate_canonical_hash(node, visitor: BaseVisitor, file_path_str: str):
    """
    The single, universal function that traverses a node, calculates its canonical
    hash, and invokes a visitor object at each step. This is the implementation
    of the unified hashing functor.
    """
    
    # Memoization cache is used to avoid re-hashing identical objects within a
    # single document traversal, which also prevents infinite recursion on circular refs.
    memo_cache = {}

    def _hash_recursive(sub_node, sub_jq_path):
        # For dicts/lists, their in-memory id() is unique for this traversal.
        # For primitives, their value is their identity.
        node_id = id(sub_node) if isinstance(sub_node, (dict, list)) else sub_node
        if node_id in memo_cache:
            return memo_cache[node_id]

        if isinstance(sub_node, dict):
            # If the hash key already exists (from a previous run), pop it.
            sub_node.pop(HASH_KEY, None)
            
            hash_inputs = []
            child_info = []
            for key in sorted(sub_node.keys()):
                child_hash = _hash_recursive(sub_node[key], f"{sub_jq_path}.{key}")
                hash_inputs.append(f"{key}:{child_hash}")
                child_info.append({'key': key, 'hash': child_hash, 'node': sub_node[key]})

            canonical_string = "{ " + ", ".join(hash_inputs) + " }"
            parent_hash = sha256_hash(canonical_string)
            
            # Now that we have the parent's hash, we can visit the parent itself
            # and then visit its children, providing them with the parent's hash.
            location = f"{file_path_str}:{sub_jq_path}"
            visitor.visit(parent_hash, sub_node, location, is_primitive=False)
            
            for child in child_info:
                is_primitive = not isinstance(child['node'], (dict, list))
                child_location = f"{file_path_str}:{sub_jq_path}.{child['key']}"
                visitor.visit(child['hash'], child['node'], child_location, is_primitive, parent_hash=parent_hash, child_key=child['key'])

        elif isinstance(sub_node, list):
            hash_inputs = []
            child_info = []
            for i, item in enumerate(sub_node):
                item_hash = _hash_recursive(item, f"{sub_jq_path}.[{i}]")
                hash_inputs.append(item_hash)
                child_info.append({'key': str(i), 'hash': item_hash, 'node': item})

            canonical_string = "[ " + ", ".join(hash_inputs) + " ]"
            parent_hash = sha256_hash(canonical_string)

            location = f"{file_path_str}:{sub_jq_path}"
            visitor.visit(parent_hash, sub_node, location, is_primitive=False)

            for child in child_info:
                is_primitive = not isinstance(child['node'], (dict, list))
                child_location = f"{file_path_str}:{sub_jq_path}.[{child['key']}]"
                visitor.visit(child['hash'], child['node'], child_location, is_primitive, parent_hash=parent_hash, child_key=child['key'])
        
        else: # It's a primitive
            primitive_string = json.dumps(sub_node, ensure_ascii=False)
            parent_hash = sha256_hash(primitive_string)
            # Primitives do not call the visitor here. They are "visited"
            # by their parent container (dict or list).

        memo_cache[node_id] = parent_hash
        return parent_hash

    # Kick off the recursive process starting at the root of the document.
    return _hash_recursive(node, ".")
