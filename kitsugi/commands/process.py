from .base import BaseCommand
from kitsugi.processing import run_asset_processor

class Command(BaseCommand):
    """
    The command responsible for processing a directory of files to build
    or update the content-addressable database.
    """
    name = "process"
    help_text = "Process a directory of files to build/update the database."
    needs_write_access = True # This command modifies the database.

    def configure_args(self):
        """Adds the 'target_directory' argument to the command's parser."""
        self.parser.add_argument(
            "target_directory",
            help="The directory of files to process."
        )

    def execute(self, repo, args):
        """
        Executes the main processing logic by calling the asset processor.
        The 'repo' object is the already-connected Repository instance.
        """
        run_asset_processor(repo, args.target_directory)