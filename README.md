# Kitsugi
Kitsugi is a Python toolkit for content-addressing, analyzing, and reconstructing structured data from fragmented files.

It acts as a "digital archaeologist," allowing you to take a directory of fragmented or related JSON files and transform them into a fully traceable, content-addressable database. This enables you to understand how data fragments are related, reconstruct complete "conceptual" documents from these fragments, and analyze the provenance of your data.

-----
## 🏛️ Core Concepts

Kitsugi is built on a few key architectural principles:

  * **Hierarchical Hashing**: Every piece of data—from a single string to a complex nested object—is given a unique SHA256 hash. This is performed by a single, universal hashing function.
  * **The Content Graph**: The relationships between parent and child hashes are stored in a simple SQLite graph database (`hash_graph` table), representing all structural relationships.
  * **Declarative Repository**: All interactions with the database are handled by a universal `execute` method that accepts a declarative request dictionary. Commands describe the data they need; the repository retrieves it.

-----
## 🚀 Installation

`kitsugi` is designed to be run as a standalone toolkit with no external dependencies beyond a standard Python 3 installation.

1.  Clone the repository to your local machine.
2.  Navigate into the `kitsugi` directory.
3.  Run the tool directly via the `kitsugi` script.

-----
## 📖 Usage

All interaction with Kitsugi is performed through the `kitsugi` command-line interface.

**General Syntax:**
`python3 kitsugi [command] [options...]`

An SQLite database file (`content_addressing.db` by default) will be created in your current directory to store the content graph.

### Command Reference

#### `coverage`

Calculate content coverage for a conceptual document and its sources.

  * **Syntax:** `python3 kitsugi coverage [-o] <doc_name>`
  * **Arguments:**
      * `<doc_name>`: The name of the conceptual document to analyze (e.g., 'doc_1').
      * `-o, --output-file`: Optional path to write a CSV report.

-----

#### `export`

Rehydrates and prints a conceptual document from the database.

  * **Syntax:** `python3 kitsugi export <doc_name>`
  * **Arguments:**
      * `<doc_name>`: The name of the document to export (e.g., 'doc_1').

-----

#### `find-path`

Find the JQ-style path between two fragment hashes.

  * **Syntax:** `python3 kitsugi find-path [--parent-hash] [--child-hash]`
  * **Arguments:**
      * `--parent-hash`: The hash of the containing (parent) fragment.
      * `--child-hash`: The hash of the contained (child) fragment.

-----

#### `find-sources`

Find original source files for a conceptual document.

  * **Syntax:** `python3 kitsugi find-sources <doc_name>`
  * **Arguments:**
      * `<doc_name>`: The name of the conceptual document to analyze (e.g., 'doc_1').

-----

#### `process`

Process a directory of files to build/update the database.

  * **Syntax:** `python3 kitsugi process <target_directory>`
  * **Arguments:**
      * `<target_directory>`: The directory of files to process.

-----

#### `reconstruct`

Reconstruct a full JSON document from a root hash.

  * **Syntax:** `python3 kitsugi reconstruct <hash>`
  * **Arguments:**
      * `<hash>`: The root hash of the document to reconstruct.

-----

#### `splice`

Find and splice shredded files to save their reconstruction recipes.

  * **Syntax:** `python3 kitsugi splice`

-----


-----
## 💡 Example Workflow

Here is a typical workflow for using Kitsugi:

1.  **Process a Directory**: Ingest a folder of JSON files into the database.
    ```bash
    python3 kitsugi process ./path/to/your/json_files
    ```
2.  **Splice Documents**: Analyze the graph to find the top-level documents.
    ```bash
    python3 kitsugi splice
    ```
    This will report the names of discovered documents, such as `doc_1`.
3.  **Export a Document**: Reconstruct a complete document and save it to a file.
    ```bash
    python3 kitsugi export doc_1 > reconstructed_document.json
    ```
4.  **Analyze Provenance**: Find out which original files were used to create your reconstructed document.
    ```bash
    python3 kitsugi find-sources reconstructed_document.json
    ```

-----
## ⚖️ License

This project is licensed under the Apache License 2.0.
