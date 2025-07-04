import json
import os
from .hashing import calculate_canonical_hash, WriteContextVisitor

def run_asset_processor(repo, root_dir: str):
    """
    Processes a directory using the universal hasher and a WriteContextVisitor,
    then saves the collected data via the repository.
    """
    # Instantiate the visitor that collects data for a database write.
    visitor = WriteContextVisitor()

    for dirpath, _, filenames in os.walk(root_dir):
        print(f"\nProcessing directory: {dirpath}")
        for filename in sorted(filenames):
            if filename.endswith(('.db', '.py')):
                continue

            full_path = os.path.join(dirpath, filename)
            relative_path = os.path.relpath(full_path, start=".")
            
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if not content.strip():
                        print(f"  -> Skipped (empty): {filename}")
                        continue
                    data = json.loads(content)
                
                # Use the single, universal hasher. It populates the visitor object.
                calculate_canonical_hash(data, visitor, relative_path)
                
                # Write back the file with the injected _sha256_hash keys
                with open(full_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                
                print(f"  -> Processed as JSON: {filename}")
            except (json.JSONDecodeError, UnicodeDecodeError):
                print(f"  -> Skipped (not JSON): {filename}")
            except Exception as e:
                print(f"    -> Error processing '{filename}': {e}")
    
    if visitor.index_data:
        print("\nSaving all collected data to database...")
        repo.clear_all_data()
        repo.save_processed_data(visitor)
        repo.commit()
    else:
        print("\nNo data to save to database.")
