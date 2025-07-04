from .base import BaseCommand
from kitsugi.analysis import find_source_files

class Command(BaseCommand):
    """
    A command to find the original source files that contributed to a
    reconstructed, conceptual document stored in the database.
    """
    name = "find-sources"
    help_text = "Find original source files for a conceptual document."
    needs_db_connection = True
    needs_write_access = False

    def configure_args(self):
        """Adds the 'doc_name' argument for this command."""
        self.parser.add_argument(
            "doc_name", 
            help="The name of the conceptual document to analyze (e.g., 'doc_1')."
        )

    def execute(self, repo, args, parser=None):
        """Executes the find_source_files logic from the analysis module."""
        find_source_files(repo, args.doc_name)
