import json
from collections import deque

def reconstruct_from_hash(repo, target_hash, memo_cache):
    """
    Recursively reconstructs a JSON object or value from its hash
    by sending declarative requests to the Repository.
    """
    if target_hash in memo_cache:
        return memo_cache[target_hash]

    # The logic now describes the data it wants: the children of the current hash.
    children_request = {
        'type': 'QUERY',
        'table': 'hash_graph',
        'select': ['child_key', 'child_hash'],
        'where': {'column': 'parent_hash', 'operator': '=', 'value': target_hash}
    }
    children = repo.execute(children_request)

    if children:
        # This reconstruction logic is pure; it only transforms the data it receives.
        is_list = all(key.isdigit() for key, _ in children)
        if is_list:
            reconstructed_list = [None] * len(children)
            for key, child_hash in children:
                reconstructed_list[int(key)] = reconstruct_from_hash(repo, child_hash, memo_cache)
            result = reconstructed_list
        else:
            reconstructed_dict = {}
            for key, child_hash in children:
                reconstructed_dict[key] = reconstruct_from_hash(repo, child_hash, memo_cache)
            result = reconstructed_dict
    else:
        # This is a leaf node. It describes its need for the primitive data.
        data_request = {
            'type': 'QUERY',
            'table': 'hash_to_data',
            'select': ['data'],
            'where': {'column': 'hash', 'operator': '=', 'value': target_hash},
            'limit': 1
        }
        data_row = repo.execute(data_request)
        
        if data_row:
            result = json.loads(data_row[0][0])
        else:
            result = {"error": "Primitive data not found for hash", "hash": target_hash}
    
    memo_cache[target_hash] = result
    return result

def find_and_splice_shredded_files(repo):
    """
    Analyzes the database via the Repository to find and save reconstruction recipes.
    """
    print("Step 1: Identifying all shredded file fragments...")
    root_hashes_req = {
        'type': 'QUERY',
        'table': 'hash_index',
        'select': ['DISTINCT hash'],
        'where': {'column': 'location', 'operator': 'LIKE', 'value': '%:.'}
    }
    root_hashes = {row[0] for row in repo.execute(root_hashes_req)}
    
    if not root_hashes:
        print("No file fragments found in the database.")
        return

    print(f"  -> Found {len(root_hashes)} unique file fragments.")

    print("\nStep 2: Finding which fragments are contained within others...")
    contained_req = {
        'type': 'QUERY',
        'table': 'hash_graph',
        'select': ['DISTINCT child_hash'],
        'where': {'column': 'child_hash', 'operator': 'IN', 'value': list(root_hashes)}
    }
    contained_hashes = {row[0] for row in repo.execute(contained_req)}

    true_roots = root_hashes - contained_hashes
    print(f"\nStep 3: Identified {len(true_roots)} final document root(s). Saving recipes...")
    
    repo.execute({'type': 'DELETE', 'table': 'reconstructed_docs'})
    
    docs_to_save = [{'doc_name': f"doc_{i+1}", 'root_hash': h} for i, h in enumerate(true_roots)]
    if docs_to_save:
        repo.execute({'type': 'INSERT', 'table': 'reconstructed_docs', 'data': docs_to_save})
    
    repo.commit()
    print(f"\nSuccessfully saved {len(docs_to_save)} document recipes.")
