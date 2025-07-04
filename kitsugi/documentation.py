import argparse
import sys

def generate_command_reference(parser):
    """Introspects the argparse parser and generates a Markdown reference."""
    
    # Access the subparsers action, which contains all the command definitions
    subparsers_actions = [
        action for action in parser._actions 
        if isinstance(action, argparse._SubParsersAction)]
    
    if not subparsers_actions:
        return ""

    markdown_lines = []
    
    for subparsers_action in subparsers_actions:
        # Sort commands alphabetically by name for a consistent order
        for name, sub_parser in sorted(subparsers_action.choices.items()):
            # Skip documenting this command itself
            if name == 'make-readme':
                continue

            markdown_lines.append(f"#### `{name}`\n")
            
            # THE FIX: Read from the '.description' attribute, which is reliably present.
            if sub_parser.description:
                markdown_lines.append(f"{sub_parser.description}\n")
            
            # --- Generate syntax line ---
            syntax = f"`python3 kitsugi {name}"
            
            positional_args = []
            optional_args = []

            for action in sub_parser._actions:
                # Separate optional (--flag) from positional arguments
                if action.option_strings:
                    # Don't document the global --help flag for each command
                    if '--help' not in action.option_strings:
                        optional_args.append(action)
                # Ensure it's a real argument, not the 'help' action
                elif action.dest != 'help':
                    positional_args.append(action)
            
            # Build the syntax string from the collected arguments
            for arg in optional_args:
                 syntax += f" [{arg.option_strings[0]}]"
            for arg in positional_args:
                syntax += f" <{arg.dest}>"
            
            syntax += "`"
            markdown_lines.append(f"  * **Syntax:** {syntax}")

            # --- Document arguments if any exist ---
            if positional_args or optional_args:
                markdown_lines.append(f"  * **Arguments:**")
                for arg in positional_args:
                    markdown_lines.append(f"      * `<{arg.dest}>`: {arg.help}")
                for arg in optional_args:
                    flags = ', '.join(arg.option_strings)
                    markdown_lines.append(f"      * `{flags}`: {arg.help}")

            markdown_lines.append("\n-----\n")
            
    return '\n'.join(markdown_lines)

def create_readme(parser, template_path, output_path):
    """
    Reads the template, generates the command reference from the given
    parser object, and writes the final README.md file.
    """
    print(f"Generating '{output_path}' from '{template_path}'...")

    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
    except FileNotFoundError:
        print(f"Error: Template file not found at '{template_path}'", file=sys.stderr)
        return

    # Generate the dynamic command reference section
    command_reference_md = generate_command_reference(parser)
    
    # Replace the placeholder in the template with the generated content
    final_content = template_content.replace('{{COMMAND_REFERENCE}}', command_reference_md)
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_content)
        print(f"  -> Successfully wrote {output_path}")
    except IOError as e:
        print(f"Error: Could not write to output file '{output_path}'. Reason: {e}", file=sys.stderr)
