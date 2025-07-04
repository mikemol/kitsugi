from .base import BaseCommand
from kitsugi.analysis import calculate_coverage

class Command(BaseCommand):
    """
    A command to calculate and report on the content coverage between a
    conceptual document and its original source fragments.
    """
    name = "coverage"
    help_text = "Calculate content coverage for a conceptual document and its sources."
    needs_db_connection = True
    needs_write_access = False

    def configure_args(self):
        """Adds the 'doc_name' and optional '--output-file' arguments."""
        self.parser.add_argument(
            "doc_name",
            help="The name of the conceptual document to analyze (e.g., 'doc_1')."
        )
        self.parser.add_argument(
            "-o", "--output-file",
            help="Optional path to write a CSV report."
        )

    def execute(self, repo, args, parser=None):
        """Executes the calculate_coverage logic from the analysis module."""
        calculate_coverage(repo, args.doc_name, args.output_file)
