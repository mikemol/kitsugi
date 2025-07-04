import argparse

class BaseCommand:
    """
    The abstract base class for all command plugins in Kitsugi.
    
    Each command inherits from this class and overrides its attributes and methods
    to define its specific behavior. This creates a standard contract that the
    main `kitsugi.py` dispatcher can rely on to automatically discover and
    register any command.
    """
    # The name of the command as invoked from the CLI (e.g., 'process')
    name = "base-command"
    
    # The one-line help text shown in the main help message
    help_text = ""
    
    # Does this command need to write to the database?
    # If False, the database will be opened in read-only mode.
    needs_write_access = False
    
    # Does this command need a database connection at all?
    # Commands like 'make-readme' can set this to False.
    needs_db_connection = True 

    def __init__(self, subparsers):
        """Initializes the command and sets up its argument parser."""
        # We pass the help_text to both 'help' and 'description' to ensure
        # it's available for the README generator to introspect reliably.
        self.parser = subparsers.add_parser(
            self.name,
            help=self.help_text,
            description=self.help_text,
            formatter_class=argparse.RawTextHelpFormatter
        )
        # Add command-specific arguments
        self.configure_args()
        # Link this command name to its own instance for the dispatcher
        self.parser.set_defaults(command_instance=self)

    def configure_args(self):
        """
        A method to be overridden by subclasses to add their specific
        command-line arguments to the parser.
        """
        pass

    def execute(self, repo, args, parser=None):
        """
        The main execution logic for the command. This method is called by
        the main dispatcher. It receives the repository instance, the parsed
        arguments, and optionally the main parser object for introspection.
        """
        raise NotImplementedError()