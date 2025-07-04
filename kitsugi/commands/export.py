import json
import sys
from .base import BaseCommand
from kitsugi.reconstruction import reconstruct_from_hash

class Command(BaseCommand):
    """
    Defines the 'export' command for rehydrating a conceptual document.
    """
    name = "export"
    help_text = "Rehydrates and prints a conceptual document from the database."
    needs_write_access = False

    def configure_args(self):
        """Adds the 'doc_name' argument required by this command."""
        self.parser.add_argument("doc_name", help="The name of the document to export (e.g., 'doc_1').")

    def execute(self, repo, args):
        """
        The main execution logic for the 'export' command.
        It finds a document's root hash and reconstructs it.
        """
        # Step 1: Describe the data needed to find the root hash.
        request = {
            'type': 'QUERY',
            'table': 'reconstructed_docs',
            'select': ['root_hash'],
            'where': {'column': 'doc_name', 'operator': '=', 'value': args.doc_name},
            'limit': 1
        }
        
        # Step 2: Send the declarative request to the repository.
        result = repo.execute(request)
        
        if not result:
            print(f"Error: Document '{args.doc_name}' not found in the database.", file=sys.stderr)
            print("Run the 'splice' command first to generate conceptual documents.", file=sys.stderr)
            return

        root_hash = result[0][0]
        
        # Print progress to stderr to keep stdout clean for the JSON output.
        print(f"Exporting '{args.doc_name}' from root hash {root_hash[:12]}...", file=sys.stderr)
        
        # Step 3: Call the reconstruction logic, passing the repository.
        reconstructed_doc = reconstruct_from_hash(repo, root_hash, memo_cache={})
        
        # Step 4: Stream the final, pure JSON to standard output.
        print(json.dumps(reconstructed_doc, indent=2))