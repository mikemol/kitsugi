import json
import sys
from .base import BaseCommand
from kitsugi.reconstruction import reconstruct_from_hash

class Command(BaseCommand):
    """
    Defines the 'reconstruct' command for the Kitsugi CLI.
    This command rebuilds a complete JSON document from a single root hash.
    """
    name = "reconstruct"
    help_text = "Reconstruct a full JSON document from a root hash."
    needs_write_access = False

    def configure_args(self):
        """Adds the required 'hash' argument to the command's parser."""
        self.parser.add_argument("hash", help="The root hash of the document to reconstruct.")

    def execute(self, repo, args):
        """

        Handles the execution of the 'reconstruct' command.
        It calls the core reconstruction logic and prints the result as JSON.
        """
        # Print status updates to stderr to keep stdout clean for the JSON output.
        print(f"Reconstructing document from root hash {args.hash[:12]}...", file=sys.stderr)
        
        reconstructed_doc = reconstruct_from_hash(repo, args.hash, memo_cache={})
        
        # Print the final, reconstructed JSON object to standard output.
        print(json.dumps(reconstructed_doc, indent=2))
