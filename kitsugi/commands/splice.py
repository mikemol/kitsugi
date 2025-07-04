from .base import BaseCommand

class Command(BaseCommand):
    """
    A command to find and splice shredded files by identifying their
    containment relationships and saving the root(s) as reconstructable documents.
    """
    name = "splice"
    help_text = "Find and splice shredded files to save their reconstruction recipes."
    needs_write_access = True # This command writes to the reconstructed_docs table.

    def configure_args(self):
        """This command takes no additional command-line arguments."""
        pass

    def execute(self, repo, args):
        """The main execution logic for the 'splice' command."""
        print("Step 1: Identifying all shredded file fragments...")
        
        # Describe the request to get all hashes that are at the root of a file.
        root_hashes_req = {
            'type': 'QUERY',
            'table': 'hash_index',
            'select': ['DISTINCT hash'],
            'where': {'column': 'location', 'operator': 'LIKE', 'value': '%:.'}
        }
        root_hashes_results = repo.execute(root_hashes_req)
        root_hashes = {row[0] for row in root_hashes_results}
        
        if not root_hashes:
            print("No file fragments found in the database.")
            return

        print(f"  -> Found {len(root_hashes)} unique file fragments.")

        print("\nStep 2: Finding which fragments are contained within others...")
        
        # Describe the request to find which of our root hashes also appear as children.
        contained_req = {
            'type': 'QUERY',
            'table': 'hash_graph',
            'select': ['DISTINCT child_hash'],
            'where': {'column': 'child_hash', 'operator': 'IN', 'value': list(root_hashes)}
        }
        contained_hashes = {row[0] for row in repo.execute(contained_req)}

        # The true roots are those that are never children of another fragment.
        true_roots = root_hashes - contained_hashes
        print(f"\nStep 3: Identified {len(true_roots)} final document root(s). Saving recipes to database...")

        # Describe the request to clear out the old recipes.
        repo.execute({'type': 'DELETE', 'table': 'reconstructed_docs'})
        
        # Prepare the new recipes for insertion.
        docs_to_save = [{'doc_name': f"doc_{i+1}", 'root_hash': h} for i, h in enumerate(sorted(list(true_roots)))]
        
        if docs_to_save:
            # Describe the request to insert the new recipes.
            repo.execute({
                'type': 'INSERT',
                'table': 'reconstructed_docs',
                'data': docs_to_save
            })
        
        # Commit the transaction to save the changes.
        repo.commit()
        print(f"\nSuccessfully saved {len(docs_to_save)} document recipes.")
