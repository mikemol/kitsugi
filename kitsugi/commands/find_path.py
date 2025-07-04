from .base import BaseCommand
from kitsugi.analysis import find_path_between_fragments

class Command(BaseCommand):
    """
    Defines the command-line interface for finding the path between two fragment hashes.
    """
    name = "find-path"
    help_text = "Find the JQ-style path between two fragment hashes."
    needs_write_access = False

    def configure_args(self):
        """Adds the --parent-hash and --child-hash arguments."""
        self.parser.add_argument(
            "--parent-hash",
            required=True,
            help="The hash of the containing (parent) fragment."
        )
        self.parser.add_argument(
            "--child-hash",
            required=True,
            help="The hash of the contained (child) fragment."
        )

    def execute(self, repo, args):
        """
        Executes the command by calling the core logic function.
        """
        find_path_between_fragments(repo, args.parent_hash, args.child_hash)
        