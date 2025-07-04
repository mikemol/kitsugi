import sys
from .base import BaseCommand
from kitsugi.documentation import create_readme
# The main 'kitsugi' module is no longer imported here to prevent circular dependencies.

class Command(BaseCommand):
    """
    A command to generate the README.md file by introspecting all other commands.
    """
    name = "make-readme"
    help_text = "Generate the README.md file from a template and command introspection."
    # This command is unique as it does not need to touch the database.
    needs_db_connection = False 

    def configure_args(self):
        """Adds the arguments specific to the make-readme command."""
        self.parser.add_argument(
            "-t", "--template", 
            default="README.md.template", 
            help="Path to the README template file."
        )
        self.parser.add_argument(
            "-o", "--output", 
            default="README.md", 
            help="Path to write the final README.md file."
        )

    def execute(self, repo, args, parser=None):
        """
        The main execution logic for the 'make-readme' command.
        It receives the fully-built main parser as an argument.
        """
        if not parser:
            print("Error: Main parser object was not correctly passed to the make-readme command.", file=sys.stderr)
            return
            
        # The core logic is delegated to the documentation module.
        create_readme(parser, args.template, args.output)
