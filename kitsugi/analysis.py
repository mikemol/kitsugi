import json
import csv
from collections import deque
from .hashing import calculate_canonical_hash, AnalysisVisitor

def get_all_constituent_hashes(repo, root_hash, all_hashes=None, visited=None):
    """
    Recursively traverses the hash_graph via the repository to find every hash
    that makes up a conceptual document, starting from its root hash.
    """
    if all_hashes is None:
        all_hashes = set()
    if visited is None:
        visited = set()

    if root_hash in visited:
        return
    
    visited.add(root_hash)
    all_hashes.add(root_hash)

    # Describe the need for the children of the current hash.
    children_request = {
        'type': 'QUERY',
        'table': 'hash_graph',
        'select': ['child_hash'],
        'where': {'column': 'parent_hash', 'operator': '=', 'value': root_hash}
    }
    children = repo.execute(children_request)

    for (child_hash,) in children:
        # Recursively call for each child.
        get_all_constituent_hashes(repo, child_hash, all_hashes, visited)

    return all_hashes

def find_source_files(repo, doc_name):
    """
    Finds all original source files for a given conceptual document name.
    """
    print(f"Step 1: Finding root hash for conceptual document '{doc_name}'...")
    
    doc_req = {
        'type': 'QUERY',
        'table': 'reconstructed_docs',
        'select': ['root_hash'],
        'where': {'column': 'doc_name', 'operator': '=', 'value': doc_name}
    }
    result = repo.execute(doc_req)
    
    if not result:
        print(f"Error: Conceptual document '{doc_name}' not found. Run 'splice' first.")
        return

    root_hash = result[0][0]
    print(f"  -> Found root hash: {root_hash[:12]}...")

    print("\nStep 2: Traversing content graph to find all constituent hashes...")
    constituent_hashes = get_all_constituent_hashes(repo, root_hash)
    print(f"  -> Found {len(constituent_hashes)} unique hashes in the conceptual document.")

    print("\nStep 3: Querying the repository for original file locations...")
    locations_request = {
        'type': 'QUERY',
        'table': 'hash_index',
        'select': ['location'],
        'where': {'column': 'hash', 'operator': 'IN', 'value': list(constituent_hashes)}
    }
    locations = repo.execute(locations_request)
    
    source_files = sorted(list({loc.split(':', 1)[0] for (loc,) in locations}))
    
    print("\n--- Source Fragment Files ---")
    for filename in source_files:
        print(f"  - {filename}")


def calculate_coverage(repo, doc_name, output_csv_path=None):
    """
    Calculates content coverage for a conceptual document and its sources.
    """
    print(f"Step 1: Analyzing conceptual document '{doc_name}' to generate its hash set...")
    doc_req = {
        'type': 'QUERY',
        'table': 'reconstructed_docs',
        'select': ['root_hash'],
        'where': {'column': 'doc_name', 'operator': '=', 'value': doc_name}
    }
    result = repo.execute(doc_req)
    
    if not result:
        print(f"Error: Conceptual document '{doc_name}' not found. Run 'splice' first.")
        return

    root_hash = result[0][0]
    print(f"  -> Found root hash: {root_hash[:12]}...")

    recon_hashes = get_all_constituent_hashes(repo, root_hash)
    print(f"  -> Found {len(recon_hashes)} unique hashes in the conceptual document.")

    print("\nStep 2: Finding contributing source files...")
    locations_request = {
        'type': 'QUERY',
        'table': 'hash_index',
        'select': ['location'],
        'where': {'column': 'hash', 'operator': 'IN', 'value': list(recon_hashes)}
    }
    locations = repo.execute(locations_request)
    source_files = sorted(list({loc.split(':', 1)[0] for (loc,) in locations}))
    
    print(f"\nStep 3: Calculating coverage for each of the {len(source_files)} source files...")
    results = []
    for filename in source_files:
        print(f"  - Analyzing fragment: {filename}")
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                source_data = json.load(f)
            
            source_visitor = AnalysisVisitor()
            calculate_canonical_hash(source_data, source_visitor, filename)
            source_hashes = source_visitor.hash_set
            
            intersection_size = len(recon_hashes.intersection(source_hashes))
            sym_diff_size = len(recon_hashes.symmetric_difference(source_hashes))
            
            results.append({
                "filename": filename,
                "intersection": intersection_size,
                "xor_difference": sym_diff_size
            })
        except Exception as e:
            print(f"    -> Warning: Could not process '{filename}'. Skipping. Reason: {e}")
            continue

    results.sort(key=lambda x: x['xor_difference'])
    
    if not output_csv_path:
        print("\n--- Coverage Analysis Results ---")
        print(f"{'Source Fragment':<70} | {'Shared Hashes (Intersection)':<30} | {'Different Hashes (XOR)':<25}")
        print("-" * 130)
        for res in results:
            print(f"{res['filename']:<70} | {res['intersection']:<30} | {res['xor_difference']:<25}")
    else:
        print(f"\n--- Writing Coverage Analysis to '{output_csv_path}' ---")
        try:
            with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['source_fragment', 'shared_hashes_intersection', 'different_hashes_xor']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for res in results:
                    writer.writerow({
                        'source_fragment': res['filename'],
                        'shared_hashes_intersection': res['intersection'],
                        'different_hashes_xor': res['xor_difference']
                    })
            print("  -> CSV file written successfully.")
        except IOError as e:
            print(f"Error: Could not write to file '{output_csv_path}'. Reason: {e}")

def find_path_between_fragments(repo, parent_hash, child_hash):
    """
    Finds the path between two fragment hashes by repeatedly sending
    declarative requests to the repository.
    """
    print(f"Searching for path from parent ({parent_hash[:12]}...) to child ({child_hash[:12]}...).")
    queue = deque([(child_hash, [])])
    visited = {child_hash}

    while queue:
        current_hash, path = queue.popleft()
        if current_hash == parent_hash:
            final_path = "." + "".join(reversed(path))
            print("\n--- Path Found ---")
            print(final_path)
            return
        
        parent_request = {
            'type': 'QUERY',
            'table': 'hash_graph',
            'select': ['parent_hash', 'child_key'],
            'where': {'column': 'child_hash', 'operator': '=', 'value': current_hash}
        }
        
        for next_parent_hash, key_in_parent in repo.execute(parent_request):
            if next_parent_hash not in visited:
                visited.add(next_parent_hash)
                new_path_segment = f"[{key_in_parent}]" if key_in_parent.isdigit() else f".{key_in_parent}"
                queue.append((next_parent_hash, path + [new_path_segment]))

    print("\n--- No Path Found ---")
    print(f"Could not find a path linking the child hash back to the specified parent hash.")
