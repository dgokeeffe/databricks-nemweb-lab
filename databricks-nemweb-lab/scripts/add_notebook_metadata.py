#!/usr/bin/env python3
"""
Add Databricks notebook metadata (base environment and dependencies) to .ipynb files.

This script adds the environmentMetadata section to notebook files so they
specify the base environment and package dependencies when run.

Usage:
    python add_notebook_metadata.py [notebook_dir]
    
    # Update all notebooks
    python add_notebook_metadata.py databricks-nemweb-lab/notebooks
    python add_notebook_metadata.py databricks-nemweb-lab/solutions
"""

import sys
import json
from pathlib import Path
from typing import Optional


def get_wheel_path() -> str:
    """
    Get the wheel path that will be deployed by the bundle.
    
    This matches the path structure used by Databricks Asset Bundles:
    /Workspace/Users/{user}/.bundle/{bundle_name}/{target}/files/{relative_path}
    """
    # Extract version from pyproject.toml using Python regex (more reliable)
    src_dir = Path(__file__).parent.parent / "src"
    version = "2.10.4"  # Default fallback
    
    try:
        import re
        with open(src_dir / "pyproject.toml", 'r') as f:
            content = f.read()
            match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
            if match:
                version = match.group(1)
    except Exception:
        pass
    
    # Bundle path structure
    wheel_name = f"nemweb_datasource-{version}-py3-none-any.whl"
    wheel_path = f"/Workspace/Users/david.okeeffe@databricks.com/.bundle/nemweb-lab/dev/files/databricks-nemweb-lab/artifacts/{wheel_name}"
    
    return wheel_path


def add_metadata_to_notebook(ipynb_file: Path, wheel_path: Optional[str] = None) -> bool:
    """Add environment metadata to a notebook file."""
    if not ipynb_file.exists():
        print(f"  ⚠ File not found: {ipynb_file}")
        return False
    
    try:
        # Read the notebook
        with open(ipynb_file, 'r', encoding='utf-8') as f:
            notebook = json.load(f)
        
        # Get or create metadata structure
        if 'metadata' not in notebook:
            notebook['metadata'] = {}
        
        # Get or create Databricks-specific metadata
        databricks_key = 'application/vnd.databricks.v1+notebook'
        if databricks_key not in notebook['metadata']:
            notebook['metadata'][databricks_key] = {}
        
        # Set environment metadata
        if wheel_path is None:
            wheel_path = get_wheel_path()
        
        notebook['metadata'][databricks_key]['environmentMetadata'] = {
            'base_environment': '',
            'dependencies': [wheel_path],
            'environment_version': '4'
        }
        
        # Ensure language is set
        if 'language_info' not in notebook['metadata']:
            notebook['metadata']['language_info'] = {
                'name': 'python'
            }
        
        # Write back
        with open(ipynb_file, 'w', encoding='utf-8') as f:
            json.dump(notebook, f, indent=1, ensure_ascii=False)
        
        print(f"  ✓ Updated {ipynb_file.name}")
        return True
        
    except Exception as e:
        print(f"  ✗ Error updating {ipynb_file.name}: {e}")
        return False


def main():
    """Add metadata to all .ipynb files in the specified directory."""
    if len(sys.argv) > 1:
        notebooks_dir = Path(sys.argv[1])
    else:
        notebooks_dir = Path(__file__).parent.parent / "notebooks"
    
    if not notebooks_dir.exists():
        print(f"Error: Directory {notebooks_dir} does not exist")
        sys.exit(1)
    
    # Find all .ipynb files
    ipynb_files = list(notebooks_dir.glob("*.ipynb"))
    
    if not ipynb_files:
        print(f"No .ipynb files found in {notebooks_dir}")
        return
    
    print(f"Found {len(ipynb_files)} notebook files in {notebooks_dir}")
    print(f"Wheel path: {get_wheel_path()}\n")
    
    updated = []
    failed = []
    
    for ipynb_file in sorted(ipynb_files):
        if add_metadata_to_notebook(ipynb_file):
            updated.append(ipynb_file)
        else:
            failed.append(ipynb_file)
    
    print(f"\n{'='*60}")
    print(f"Metadata update complete!")
    print(f"  Updated: {len(updated)} files")
    if failed:
        print(f"  Failed: {len(failed)} files")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
