#!/usr/bin/env python3
"""
Convert Databricks notebook format .py files to .ipynb format.

This script converts Python files that start with '# Databricks notebook source'
to Jupyter notebook (.ipynb) format for better compatibility with Databricks Asset Bundles.

Usage:
    python convert_py_to_ipynb.py [notebooks_dir] [output_dir]
    
    # Convert all notebooks in databricks-nemweb-lab/notebooks/
    python convert_py_to_ipynb.py databricks-nemweb-lab/notebooks databricks-nemweb-lab/notebooks
    
    # Convert solutions too
    python convert_py_to_ipynb.py databricks-nemweb-lab/solutions databricks-nemweb-lab/solutions
"""

import sys
import json
import re
from pathlib import Path
from typing import List, Tuple


def parse_databricks_notebook(py_content: str) -> List[dict]:
    """
    Parse a Databricks notebook format Python file into notebook cells.
    
    Databricks notebook format:
    - Lines starting with '# Databricks notebook source' indicate a new cell
    - Lines starting with '# MAGIC' are magic commands (like %md, %sql, etc.)
    - Regular Python code lines are code cells
    """
    cells = []
    current_cell = []
    current_cell_type = 'code'
    
    lines = py_content.split('\n')
    
    for line in lines:
        # Check for magic commands (markdown, SQL, etc.)
        if line.strip().startswith('# MAGIC'):
            # Extract the magic command
            magic_content = line.replace('# MAGIC', '').strip()
            
            # Determine cell type from magic command
            if magic_content.startswith('%md'):
                if current_cell and current_cell_type == 'code':
                    # Save previous code cell
                    cells.append({
                        'cell_type': 'code',
                        'source': '\n'.join(current_cell),
                        'metadata': {},
                        'outputs': [],
                        'execution_count': None
                    })
                    current_cell = []
                
                current_cell_type = 'markdown'
                # Remove %md prefix and add content
                md_content = magic_content.replace('%md', '').strip()
                if md_content:
                    current_cell.append(md_content)
            elif magic_content.startswith('%sql'):
                if current_cell and current_cell_type == 'code':
                    cells.append({
                        'cell_type': 'code',
                        'source': '\n'.join(current_cell),
                        'metadata': {},
                        'outputs': [],
                        'execution_count': None
                    })
                    current_cell = []
                
                current_cell_type = 'code'
                current_cell.append(magic_content)
            elif magic_content.startswith('%python') or magic_content.startswith('%scala') or magic_content.startswith('%r'):
                if current_cell and current_cell_type == 'code':
                    cells.append({
                        'cell_type': 'code',
                        'source': '\n'.join(current_cell),
                        'metadata': {},
                        'outputs': [],
                        'execution_count': None
                    })
                    current_cell = []
                
                current_cell_type = 'code'
                current_cell.append(magic_content)
            else:
                # Other magic commands - treat as code
                if current_cell_type == 'markdown':
                    # Save markdown cell
                    cells.append({
                        'cell_type': 'markdown',
                        'source': '\n'.join(current_cell),
                        'metadata': {}
                    })
                    current_cell = []
                    current_cell_type = 'code'
                
                current_cell.append(magic_content)
        elif line.strip().startswith('# Databricks notebook source'):
            # New notebook marker - save current cell if any
            if current_cell:
                if current_cell_type == 'markdown':
                    cells.append({
                        'cell_type': 'markdown',
                        'source': '\n'.join(current_cell),
                        'metadata': {}
                    })
                else:
                    cells.append({
                        'cell_type': 'code',
                        'source': '\n'.join(current_cell),
                        'metadata': {},
                        'outputs': [],
                        'execution_count': None
                    })
                current_cell = []
            current_cell_type = 'code'
            # Skip the header line
            continue
        elif line.strip().startswith('# COMMAND ----------'):
            # Cell separator in Databricks format
            if current_cell:
                if current_cell_type == 'markdown':
                    cells.append({
                        'cell_type': 'markdown',
                        'source': '\n'.join(current_cell),
                        'metadata': {}
                    })
                else:
                    cells.append({
                        'cell_type': 'code',
                        'source': '\n'.join(current_cell),
                        'metadata': {},
                        'outputs': [],
                        'execution_count': None
                    })
                current_cell = []
            current_cell_type = 'code'
        else:
            # Regular content line
            if current_cell_type == 'markdown':
                # Continue markdown cell
                current_cell.append(line)
            else:
                # Code cell
                current_cell.append(line)
    
    # Add final cell
    if current_cell:
        if current_cell_type == 'markdown':
            cells.append({
                'cell_type': 'markdown',
                'source': '\n'.join(current_cell),
                'metadata': {}
            })
        else:
            cells.append({
                'cell_type': 'code',
                'source': '\n'.join(current_cell),
                'metadata': {},
                'outputs': [],
                'execution_count': None
            })
    
    return cells


def convert_py_to_ipynb(py_file: Path, output_dir: Path) -> Path:
    """Convert a single .py file to .ipynb format."""
    print(f"Converting {py_file.name}...")
    
    # Read the Python file
    with open(py_file, 'r', encoding='utf-8') as f:
        py_content = f.read()
    
    # Check if it's a Databricks notebook
    if not py_content.startswith('# Databricks notebook source'):
        print(f"  ⚠ Skipping {py_file.name} - not a Databricks notebook")
        return None
    
    # Parse into cells
    cells = parse_databricks_notebook(py_content)
    
    if not cells:
        print(f"  ⚠ No cells found in {py_file.name}")
        return None
    
    # Create notebook structure
    notebook = {
        'cells': cells,
        'metadata': {
            'language_info': {
                'name': 'python',
                'version': '3.10.0'
            },
            'orig_nbformat': 4
        },
        'nbformat': 4,
        'nbformat_minor': 4
    }
    
    # Write .ipynb file
    ipynb_file = output_dir / f"{py_file.stem}.ipynb"
    with open(ipynb_file, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=1, ensure_ascii=False)
    
    print(f"  ✓ Created {ipynb_file.name}")
    return ipynb_file


def main():
    """Convert all .py notebook files to .ipynb format."""
    if len(sys.argv) > 1:
        notebooks_dir = Path(sys.argv[1])
    else:
        notebooks_dir = Path(__file__).parent.parent / "notebooks"
    
    if len(sys.argv) > 2:
        output_dir = Path(sys.argv[2])
    else:
        output_dir = notebooks_dir
    
    if not notebooks_dir.exists():
        print(f"Error: Directory {notebooks_dir} does not exist")
        sys.exit(1)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all .py files
    py_files = list(notebooks_dir.glob("*.py"))
    
    if not py_files:
        print(f"No .py files found in {notebooks_dir}")
        return
    
    print(f"Found {len(py_files)} Python files in {notebooks_dir}")
    print(f"Output directory: {output_dir}\n")
    
    converted = []
    skipped = []
    
    for py_file in sorted(py_files):
        result = convert_py_to_ipynb(py_file, output_dir)
        if result:
            converted.append(result)
        else:
            skipped.append(py_file)
    
    print(f"\n{'='*60}")
    print(f"Conversion complete!")
    print(f"  Converted: {len(converted)} files")
    if skipped:
        print(f"  Skipped: {len(skipped)} files")
    print(f"{'='*60}")
    
    if converted:
        print("\nConverted files:")
        for f in converted:
            print(f"  - {f.name}")


if __name__ == "__main__":
    main()
