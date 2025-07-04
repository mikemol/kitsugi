#!/usr/bin/env python3
import argparse
import sys
import os
import pkgutil
import importlib

# This allows the script to be run from the top-level project directory
# and find the 'kitsugi' module within it.
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from kitsugi.database import connect_to_db, Repository

def create_parser():
    """
    Builds the main parser and discovers/registers all command objects.
    
    This function scans the kitsugi.commands package, imports each module,
    and initializes the 'Command' class within it, allowing for a fully
    modular, plugin-based architecture.
    """
    parser = argparse.ArgumentParser(
        description="Kitsugi: A content-addressable storage and analysis tool for JSON data.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--db",
        default="content_addressing.db",
        help="Path to the SQLite database file. (default: content_addressing.db)"
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # --- Automatic Command Discovery ---
    import kitsugi.commands
    command_path = kitsugi.commands.__path__
    module_prefix = f"{kitsugi.commands.__name__}."
    
    discovered_commands = {}

    for _, name, _ in pkgutil.iter_modules(command_path, prefix=module_prefix):
        module = importlib.import_module(name)
        
        if hasattr(module, 'Command'):
            command_class = module.Command
            instance = command_class(subparsers)
            discovered_commands[instance.name] = instance

    return parser, discovered_commands

def main():
    """
    Main entry point for the Kitsugi tool. It parses arguments, sets up the
    database connection based on command requirements, and executes the command.
    """
    parser, commands = create_parser()
    
    # Hook argcomplete into the fully constructed parser
    try:
        import argcomplete
        argcomplete.autocomplete(parser)
    except ImportError:
        # argcomplete is an optional dependency, so we just pass if it's not installed
        pass

    args = parser.parse_args()

    command_to_run = args.command_instance
    
    # Handle commands that don't need a DB connection (like 'make-readme')
    if not getattr(command_to_run, 'needs_db_connection', True):
        command_to_run.execute(None, args, parser)
        print("\n✨ Done.")
        return

    # Handle commands that require a database connection
    read_only = not command_to_run.needs_write_access
    conn = connect_to_db(args.db, read_only=read_only)
    if not conn:
        sys.exit(1)
    
    repo = Repository(conn)
    
    try:
        # Call the execute method on the command instance, passing all necessary context
        command_to_run.execute(repo, args, parser)
    except Exception as e:
        import traceback
        print(f"\nAn error occurred during execution of command '{args.command}':", file=sys.stderr)
        print(f"{e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
    
    print("\n✨ Done.")

if __name__ == "__main__":
    main()
