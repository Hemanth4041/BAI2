"""
Command-line entry point to execute the BAI to BigQuery pipeline.

This script serves as a user-facing wrapper around the main application logic.
It handles setting up the Python path correctly for development environments,
allowing the pipeline to be run directly without prior installation.

Usage:
    From the project root directory, run:
    python scripts/run_pipeline.py "gcs-bucket-name/path/to/your-file.bai"
"""

import sys
import os

# --- Path Setup for Development ---
# This block is a standard practice for making a project's source code available
# to a script that lives outside the main package directory.
#
# 1. It finds the absolute path to the directory containing this script (`scripts/`).
# 2. It goes up one level to get the project's root directory (`bai_pipeline/`).
# 3. It constructs the path to the `src` directory.
# 4. It adds the `src` directory to the beginning of Python's path list.
#
# This ensures that the `import bai_pipeline` statement will find your code
# in `src/bai_pipeline/` when you run this script from the command line.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# --- End Path Setup ---


def main():
    """
    Imports and executes the main command-line interface function from the application.
    """
    # We import here, after the path has been configured.
    from bai_pipeline.main import run_cli

    print("--- BAI Pipeline Runner ---")
    
    # The run_cli function is designed to handle everything else:
    # - Parsing command-line arguments
    # - Calling the main pipeline logic
    # - Handling exceptions and setting the exit code
    run_cli()


if __name__ == "__main__":
    main()